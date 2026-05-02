"""
Distributed Lock Manager
-------------------------
Implements distributed locking via Raft consensus with:
  - Shared (S) and Exclusive (X) lock modes
  - Deadlock detection using wait-for graph cycle detection
  - Automatic lock expiry
  - Network partition handling (locks only granted through leader)
"""

import asyncio
import logging
import time
from collections import defaultdict
from typing import Dict, List, Optional, Set

from aiohttp import web

from src.nodes.base_node import BaseNode
from src.utils.metrics import METRICS

logger = logging.getLogger(__name__)

LOCK_SHARED = "S"
LOCK_EXCLUSIVE = "X"


class LockEntry:
    def __init__(self, key: str, mode: str, client_id: str, timeout: float):
        self.key = key
        self.mode = mode
        self.client_id = client_id
        self.acquired_at = time.monotonic()
        self.timeout = timeout

    def is_expired(self) -> bool:
        return (time.monotonic() - self.acquired_at) > self.timeout

    def compatible(self, requested_mode: str) -> bool:
        """Shared locks are compatible with each other; exclusive is incompatible with any."""
        if requested_mode == LOCK_EXCLUSIVE:
            return False
        return self.mode == LOCK_SHARED


class WaitForGraph:
    """Directed graph to detect deadlocks via DFS cycle detection."""

    def __init__(self):
        self._edges: Dict[str, Set[str]] = defaultdict(set)

    def add_wait(self, waiter: str, holder: str):
        self._edges[waiter].add(holder)

    def remove_client(self, client_id: str):
        self._edges.pop(client_id, None)
        for waiters in self._edges.values():
            waiters.discard(client_id)

    def has_cycle(self) -> Optional[List[str]]:
        """Return a cycle if one exists, else None."""
        visited: Set[str] = set()
        path: List[str] = []

        def dfs(node: str) -> bool:
            if node in path:
                cycle_start = path.index(node)
                return path[cycle_start:]
            if node in visited:
                return None
            visited.add(node)
            path.append(node)
            for neighbor in self._edges.get(node, []):
                result = dfs(neighbor)
                if result is not None:
                    return result
            path.pop()
            return None

        for node in list(self._edges.keys()):
            result = dfs(node)
            if result:
                return result
        return None

    def clear(self):
        self._edges.clear()


class LockManagerNode(BaseNode):
    def __init__(self, node_id: str, host: str, port: int, peers: List[str],
                 lock_timeout: float = 30.0, deadlock_check_interval: float = 5.0):
        super().__init__(node_id, host, port, peers)
        self.lock_timeout = lock_timeout
        self.deadlock_check_interval = deadlock_check_interval

        # State machine (replicated via Raft)
        # key → list of LockEntry (multiple shared holders allowed)
        self._locks: Dict[str, List[LockEntry]] = defaultdict(list)
        # client_id → keys it holds
        self._client_locks: Dict[str, Set[str]] = defaultdict(set)
        # Pending waiters: key → list of (client_id, mode, event)
        self._waiters: Dict[str, List] = defaultdict(list)

        self._wait_for_graph = WaitForGraph()
        self._lock = asyncio.Lock()

        self._setup_lock_routes()

    def _setup_lock_routes(self):
        self.app.router.add_post("/lock/acquire", self._handle_acquire)
        self.app.router.add_post("/lock/release", self._handle_release)
        self.app.router.add_get("/lock/status", self._handle_lock_status)

    async def start(self):
        await super().start()
        asyncio.create_task(self._expiry_loop())
        asyncio.create_task(self._deadlock_detection_loop())

    # ─────────────────────────────────────────────
    # HTTP Handlers
    # ─────────────────────────────────────────────

    async def _handle_acquire(self, request: web.Request) -> web.Response:
        body = await request.json()
        key = body.get("key")
        mode = body.get("mode", LOCK_EXCLUSIVE).upper()
        client_id = body.get("client_id")
        wait = body.get("wait", True)  # block until acquired?

        if not key or not client_id:
            return web.json_response({"error": "key and client_id required"}, status=400)
        if mode not in (LOCK_SHARED, LOCK_EXCLUSIVE):
            return web.json_response({"error": "mode must be S or X"}, status=400)

        if not self.is_leader():
            return web.json_response({
                "error": "not leader",
                "leader_id": self.raft.leader_id,
            }, status=409)

        # Propose through Raft
        result = await self._try_acquire(key, mode, client_id, wait=wait)
        return web.json_response(result)

    async def _handle_release(self, request: web.Request) -> web.Response:
        body = await request.json()
        key = body.get("key")
        client_id = body.get("client_id")

        if not key or not client_id:
            return web.json_response({"error": "key and client_id required"}, status=400)

        if not self.is_leader():
            return web.json_response({"error": "not leader", "leader_id": self.raft.leader_id}, status=409)

        success = await self.propose({"op": "release", "key": key, "client_id": client_id})
        if success:
            METRICS.counter("lock_releases_total").inc()
        return web.json_response({"released": success})

    async def _handle_lock_status(self, request: web.Request) -> web.Response:
        async with self._lock:
            held = {
                key: [{"client": e.client_id, "mode": e.mode} for e in entries]
                for key, entries in self._locks.items() if entries
            }
        return web.json_response({"locks": held, "node_id": self.node_id})

    # ─────────────────────────────────────────────
    # Lock logic
    # ─────────────────────────────────────────────

    async def _try_acquire(self, key: str, mode: str, client_id: str, wait: bool) -> dict:
        async with self._lock:
            if self._can_acquire(key, mode, client_id):
                # Update local state DULU supaya request berikutnya tahu lock sudah dipegang
                entry = LockEntry(key, mode, client_id, self.lock_timeout)
                self._locks[key].append(entry)
                self._client_locks[client_id].add(key)
                METRICS.counter("lock_acquires_total").inc()
                # Replicate ke Raft di background
                asyncio.create_task(self.propose({"op": "acquire", "key": key, "mode": mode, "client_id": client_id}))
                return {"acquired": True, "key": key, "mode": mode}
            else:
                # Update wait-for graph
                for holder in self._locks[key]:
                    self._wait_for_graph.add_wait(client_id, holder.client_id)

        if not wait:
            METRICS.counter("lock_contention_total").inc()
            return {"acquired": False, "key": key, "reason": "lock held"}

        # Wait for lock to become available
        event = asyncio.Event()
        async with self._lock:
            self._waiters[key].append((client_id, mode, event))

        try:
            await asyncio.wait_for(event.wait(), timeout=self.lock_timeout)
            async with self._lock:
                if self._can_acquire(key, mode, client_id):
                    await self.propose({"op": "acquire", "key": key, "mode": mode, "client_id": client_id})
                    METRICS.counter("lock_acquires_total").inc()
                    return {"acquired": True, "key": key, "mode": mode}
        except asyncio.TimeoutError:
            async with self._lock:
                self._waiters[key] = [(c, m, e) for c, m, e in self._waiters[key] if c != client_id]
            METRICS.counter("lock_timeouts_total").inc()
            return {"acquired": False, "key": key, "reason": "timeout"}

        return {"acquired": False, "key": key, "reason": "failed"}

    def _can_acquire(self, key: str, mode: str, client_id: str) -> bool:
        holders = self._locks.get(key, [])
        if not holders:
            return True
        # Client re-acquiring its own lock is allowed
        if all(e.client_id == client_id for e in holders):
            return True
        # Shared locks compatible with shared
        if mode == LOCK_SHARED and all(e.mode == LOCK_SHARED for e in holders):
            return True
        return False

    # ─────────────────────────────────────────────
    # Raft state machine apply
    # ─────────────────────────────────────────────

    async def apply_command(self, command: dict):
        op = command.get("op")
        key = command.get("key")
        client_id = command.get("client_id")

        async with self._lock:
            if op == "acquire":
                mode = command.get("mode", LOCK_EXCLUSIVE)
                # Only grant lock if it can be acquired
                if self._can_acquire(key, mode, client_id):
                    entry = LockEntry(key, mode, client_id, self.lock_timeout)
                    self._locks[key].append(entry)
                    self._client_locks[client_id].add(key)
                    self._wait_for_graph.remove_client(client_id)
                    logger.debug(f"[{self.node_id}] APPLY acquire key={key} client={client_id} mode={mode}")
                else:
                    logger.debug(f"[{self.node_id}] APPLY acquire REJECTED key={key} client={client_id} mode={mode}")

            elif op == "release":
                self._locks[key] = [e for e in self._locks[key] if e.client_id != client_id]
                self._client_locks[client_id].discard(key)
                self._wait_for_graph.remove_client(client_id)
                logger.debug(f"[{self.node_id}] APPLY release key={key} client={client_id}")
                # Notify waiters
                await self._notify_waiters(key)

    async def _notify_waiters(self, key: str):
        for client_id, mode, event in list(self._waiters.get(key, [])):
            if self._can_acquire(key, mode, client_id):
                event.set()
                self._waiters[key] = [(c, m, e) for c, m, e in self._waiters[key] if c != client_id]
                break  # Notify one at a time (exclusive semantics)

    # ─────────────────────────────────────────────
    # Background tasks
    # ─────────────────────────────────────────────

    async def _expiry_loop(self):
        """Periodically expire timed-out locks."""
        while True:
            await asyncio.sleep(5)
            async with self._lock:
                for key in list(self._locks.keys()):
                    expired = [e for e in self._locks[key] if e.is_expired()]
                    for e in expired:
                        self._locks[key].remove(e)
                        self._client_locks[e.client_id].discard(key)
                        METRICS.counter("lock_expirations_total").inc()
                        logger.warning(f"[{self.node_id}] Lock expired: key={key} client={e.client_id}")
                    if expired:
                        await self._notify_waiters(key)

    async def _deadlock_detection_loop(self):
        """Detect and resolve deadlocks by aborting the youngest waiter."""
        while True:
            await asyncio.sleep(self.deadlock_check_interval)
            async with self._lock:
                cycle = self._wait_for_graph.has_cycle()
                if cycle:
                    METRICS.counter("deadlocks_detected_total").inc()
                    victim = cycle[-1]  # Abort the last node in the cycle
                    logger.warning(f"[{self.node_id}] Deadlock detected! Cycle={cycle}, aborting victim={victim}")
                    # Remove victim from all wait queues
                    for key in list(self._waiters.keys()):
                        self._waiters[key] = [
                            (c, m, e) for c, m, e in self._waiters[key]
                            if not (c == victim and e.set() is None)
                        ]
                    self._wait_for_graph.remove_client(victim)