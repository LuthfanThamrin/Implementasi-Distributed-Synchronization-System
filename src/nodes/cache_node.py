import asyncio
import logging
import time
from collections import OrderedDict, defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from aiohttp import web

from src.nodes.base_node import BaseNode
from src.utils.metrics import METRICS

logger = logging.getLogger(__name__)


class MESIState(Enum):
    MODIFIED = "M"
    EXCLUSIVE = "E"
    SHARED = "S"
    INVALID = "I"


@dataclass
class CacheEntry:
    key: str
    value: Any
    state: MESIState = MESIState.EXCLUSIVE
    access_count: int = 0
    last_access: float = field(default_factory=time.time)
    created_at: float = field(default_factory=time.time)

    def touch(self):
        self.access_count += 1
        self.last_access = time.time()


class LRUCache:
    
    def __init__(self, max_size: int):
        self.max_size = max_size
        self._store: OrderedDict[str, CacheEntry] = OrderedDict()

    def get(self, key: str) -> Optional[CacheEntry]:
        if key not in self._store:
            return None
        self._store.move_to_end(key)
        entry = self._store[key]
        entry.touch()
        return entry

    def put(self, key: str, entry: CacheEntry) -> Optional[str]:
        """Returns evicted key if eviction occurred."""
        evicted = None
        if key in self._store:
            self._store.move_to_end(key)
            self._store[key] = entry
        else:
            if len(self._store) >= self.max_size:
                evicted, _ = self._store.popitem(last=False)
            self._store[key] = entry
        return evicted

    def remove(self, key: str) -> bool:
        if key in self._store:
            del self._store[key]
            return True
        return False

    def keys(self):
        return list(self._store.keys())

    def __contains__(self, key):
        return key in self._store

    def __len__(self):
        return len(self._store)


class LFUCache:
    
    def __init__(self, max_size: int):
        self.max_size = max_size
        self._store: Dict[str, CacheEntry] = {}

    def get(self, key: str) -> Optional[CacheEntry]:
        if key not in self._store:
            return None
        self._store[key].touch()
        return self._store[key]

    def put(self, key: str, entry: CacheEntry) -> Optional[str]:
        evicted = None
        if key not in self._store and len(self._store) >= self.max_size:
            # Evict least frequently used
            lfu_key = min(self._store, key=lambda k: self._store[k].access_count)
            del self._store[lfu_key]
            evicted = lfu_key
        self._store[key] = entry
        return evicted

    def remove(self, key: str) -> bool:
        if key in self._store:
            del self._store[key]
            return True
        return False

    def keys(self):
        return list(self._store.keys())

    def __contains__(self, key):
        return key in self._store

    def __len__(self):
        return len(self._store)


class CacheNode(BaseNode):
    def __init__(self, node_id: str, host: str, port: int, peers: List[str],
                 max_size: int = 1000, replacement_policy: str = "LRU"):
        super().__init__(node_id, host, port, peers)
        self.max_size = max_size
        self.replacement_policy = replacement_policy.upper()

        if self.replacement_policy == "LFU":
            self._cache: Any = LFUCache(max_size)
        else:
            self._cache: Any = LRUCache(max_size)

        # MESI state per key (this node's view)
        self._mesi_states: Dict[str, MESIState] = {}
        self._lock = asyncio.Lock()

        self._setup_cache_routes()

    def _setup_cache_routes(self):
        self.app.router.add_post("/cache/put", self._handle_put)
        self.app.router.add_post("/cache/get", self._handle_get)
        self.app.router.add_post("/cache/delete", self._handle_delete)
        self.app.router.add_post("/cache/invalidate", self._handle_invalidate)
        self.app.router.add_get("/cache/status", self._handle_cache_status)
        # Inter-node MESI coherence messages
        self.app.router.add_post("/cache/coherence/invalidate", self._handle_coherence_invalidate)
        self.app.router.add_post("/cache/coherence/share", self._handle_coherence_share)

    # HTTP Handlers
    async def _handle_put(self, request: web.Request) -> web.Response:
        if not self.is_leader():
            return web.json_response({"error": "not leader", "leader_id": self.raft.leader_id}, status=409)

        body = await request.json()
        key = body.get("key")
        value = body.get("val") or body.get("value")
        if not key:
            return web.json_response({"error": "key required"}, status=400)

        success = await self.propose({"op": "put", "key": key, "value": value})
        if success:
            METRICS.counter("cache_puts_total").inc()
            # Invalidate other caches (MESI: M state)
            asyncio.create_task(self._broadcast_invalidate(key))
        return web.json_response({"stored": success, "key": key})

    async def _handle_get(self, request: web.Request) -> web.Response:
        body = await request.json()
        key = body.get("key")
        if not key:
            return web.json_response({"error": "key required"}, status=400)

        async with self._lock:
            state = self._mesi_states.get(key, MESIState.INVALID)
            if state == MESIState.INVALID or key not in self._cache:
                METRICS.counter("cache_misses_total").inc()
                return web.json_response({"hit": False, "key": key, "value": None})

            entry = self._cache.get(key)
            if entry:
                METRICS.counter("cache_hits_total").inc()
                return web.json_response({
                    "hit": True,
                    "key": key,
                    "value": entry.value,
                    "state": state.value,
                    "access_count": entry.access_count,
                })

        METRICS.counter("cache_misses_total").inc()
        return web.json_response({"hit": False, "key": key, "value": None})

    async def _handle_delete(self, request: web.Request) -> web.Response:
        if not self.is_leader():
            return web.json_response({"error": "not leader"}, status=409)

        body = await request.json()
        key = body.get("key")
        success = await self.propose({"op": "delete", "key": key})
        if success:
            asyncio.create_task(self._broadcast_invalidate(key))
        return web.json_response({"deleted": success, "key": key})

    async def _handle_invalidate(self, request: web.Request) -> web.Response:
        body = await request.json()
        key = body.get("key")
        async with self._lock:
            self._mesi_states[key] = MESIState.INVALID
        return web.json_response({"invalidated": True, "key": key})

    async def _handle_cache_status(self, request: web.Request) -> web.Response:
        async with self._lock:
            states = {k: v.value for k, v in self._mesi_states.items()}
        return web.json_response({
            "size": len(self._cache),
            "max_size": self.max_size,
            "policy": self.replacement_policy,
            "mesi_states": states,
            "node_id": self.node_id,
            "is_leader": self.is_leader(),
        })

    async def _handle_coherence_invalidate(self, request: web.Request) -> web.Response:
        """Peer asks us to invalidate a key (MESI: S → I or E → I)."""
        body = await request.json()
        key = body.get("key")
        async with self._lock:
            self._mesi_states[key] = MESIState.INVALID
            METRICS.counter("cache_coherence_invalidations_total").inc()
        logger.debug(f"[{self.node_id}] Coherence invalidate key={key}")
        return web.json_response({"ok": True})

    async def _handle_coherence_share(self, request: web.Request) -> web.Response:
        """Peer notifies us data is shared (MESI: E → S)."""
        body = await request.json()
        key = body.get("key")
        async with self._lock:
            if self._mesi_states.get(key) == MESIState.EXCLUSIVE:
                self._mesi_states[key] = MESIState.SHARED
        return web.json_response({"ok": True})

   
    # Raft state machine apply
    

    async def apply_command(self, command: dict):
        op = command.get("op")
        key = command.get("key")

        async with self._lock:
            if op == "put":
                value = command.get("value")
                entry = CacheEntry(key=key, value=value, state=MESIState.MODIFIED)
                evicted = self._cache.put(key, entry)
                self._mesi_states[key] = MESIState.MODIFIED
                if evicted:
                    self._mesi_states.pop(evicted, None)
                    METRICS.counter("cache_evictions_total").inc()
                METRICS.gauge("cache_size").set(len(self._cache))
                logger.debug(f"[{self.node_id}] APPLY put key={key}")

            elif op == "delete":
                self._cache.remove(key)
                self._mesi_states.pop(key, None)
                METRICS.gauge("cache_size").set(len(self._cache))
                logger.debug(f"[{self.node_id}] APPLY delete key={key}")

            elif op == "invalidate_state":
                self._mesi_states[key] = MESIState.INVALID

    
    # MESI coherence broadcasts

    async def _broadcast_invalidate(self, key: str):
        """Send invalidation to all peer caches (MESI write-invalidate protocol)."""
        import aiohttp
        for peer in self.peers:
            url = f"http://{peer}/cache/coherence/invalidate"
            try:
                async with aiohttp.ClientSession() as session:
                    await session.post(url, json={"key": key},
                                       timeout=aiohttp.ClientTimeout(total=1.0))
            except Exception:
                pass