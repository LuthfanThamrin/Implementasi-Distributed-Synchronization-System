import asyncio
import hashlib
import logging
import time
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional

from aiohttp import web

from src.nodes.base_node import BaseNode
from src.utils.metrics import METRICS

logger = logging.getLogger(__name__)


@dataclass
class Message:
    msg_id: str
    key: str
    value: Any
    producer_id: str
    enqueued_at: float = field(default_factory=time.time)
    delivery_count: int = 0
    acked: bool = False
    in_flight: bool = False
    in_flight_at: Optional[float] = None


def consistent_hash(key: str, nodes: int) -> int:
    """Map a key to a partition index using consistent hashing."""
    digest = int(hashlib.md5(key.encode()).hexdigest(), 16)
    return digest % nodes


class QueueNode(BaseNode):
    def __init__(self, node_id: str, host: str, port: int, peers: List[str],
                 max_size: int = 10000, message_ttl: int = 3600, ack_timeout: int = 30):
        super().__init__(node_id, host, port, peers)
        self.max_size = max_size
        self.message_ttl = message_ttl
        self.ack_timeout = ack_timeout

        # State (replicated via Raft)
        self._queue: OrderedDict[str, Message] = OrderedDict()  # msg_id → Message
        self._in_flight: Dict[str, Message] = {}  # msg_id → Message (delivered, awaiting ACK)
        self._lock = asyncio.Lock()

        self._setup_queue_routes()

    def _setup_queue_routes(self):
        self.app.router.add_post("/queue/enq", self._handle_enqueue)
        self.app.router.add_post("/queue/deq", self._handle_dequeue)
        self.app.router.add_post("/queue/ack", self._handle_ack)
        self.app.router.add_post("/queue/nack", self._handle_nack)
        self.app.router.add_get("/queue/status", self._handle_queue_status)

    async def start(self):
        await super().start()
        asyncio.create_task(self._redelivery_loop())
        asyncio.create_task(self._ttl_cleanup_loop())


    # HTTP Handlers
    async def _handle_enqueue(self, request: web.Request) -> web.Response:
        if not self.is_leader():
            return web.json_response({"error": "not leader", "leader_id": self.raft.leader_id}, status=409)

        body = await request.json()
        key = body.get("key", "default")
        value = body.get("value")
        producer_id = body.get("producer_id", "anonymous")

        async with self._lock:
            if len(self._queue) >= self.max_size:
                METRICS.counter("queue_dropped_total").inc()
                return web.json_response({"error": "queue full"}, status=503)

        msg_id = str(uuid.uuid4())
        command = {
            "op": "enqueue",
            "msg_id": msg_id,
            "key": key,
            "value": value,
            "producer_id": producer_id,
        }
        success = await self.propose(command)
        if success:
            METRICS.counter("queue_enq_total").inc()
            return web.json_response({"enqueued": True, "msg_id": msg_id})
        return web.json_response({"error": "replication failed"}, status=500)

    async def _handle_dequeue(self, request: web.Request) -> web.Response:
        if not self.is_leader():
            return web.json_response({"error": "not leader", "leader_id": self.raft.leader_id}, status=409)

        body = {}
        try:
            body = await request.json()
        except Exception:
            pass
        consumer_id = body.get("consumer_id", "anonymous")
        key_filter = body.get("key")  # Optional: only dequeue messages for this key

        async with self._lock:
            msg = self._pick_message(key_filter)
            if msg is None:
                return web.json_response({"message": None, "empty": True})
            msg_id = msg.msg_id

        command = {"op": "dequeue", "msg_id": msg_id, "consumer_id": consumer_id}
        success = await self.propose(command)
        if success:
            METRICS.counter("queue_deq_total").inc()
            async with self._lock:
                in_flight = self._in_flight.get(msg_id)
            if in_flight:
                return web.json_response({
                    "msg_id": in_flight.msg_id,
                    "key": in_flight.key,
                    "value": in_flight.value,
                    "delivery_count": in_flight.delivery_count,
                })
        return web.json_response({"error": "replication failed"}, status=500)

    async def _handle_ack(self, request: web.Request) -> web.Response:
        if not self.is_leader():
            return web.json_response({"error": "not leader", "leader_id": self.raft.leader_id}, status=409)

        body = await request.json()
        msg_id = body.get("msg_id")
        if not msg_id:
            return web.json_response({"error": "msg_id required"}, status=400)

        success = await self.propose({"op": "ack", "msg_id": msg_id})
        if success:
            METRICS.counter("queue_ack_total").inc()
        return web.json_response({"acked": success, "msg_id": msg_id})

    async def _handle_nack(self, request: web.Request) -> web.Response:
        if not self.is_leader():
            return web.json_response({"error": "not leader"}, status=409)

        body = await request.json()
        msg_id = body.get("msg_id")
        success = await self.propose({"op": "nack", "msg_id": msg_id})
        return web.json_response({"nacked": success, "msg_id": msg_id})

    async def _handle_queue_status(self, request: web.Request) -> web.Response:
        async with self._lock:
            return web.json_response({
                "queued": len(self._queue),
                "in_flight": len(self._in_flight),
                "max_size": self.max_size,
                "node_id": self.node_id,
                "is_leader": self.is_leader(),
            })


    # State machine apply
    async def apply_command(self, command: dict):
        op = command.get("op")

        async with self._lock:
            if op == "enqueue":
                msg = Message(
                    msg_id=command["msg_id"],
                    key=command["key"],
                    value=command["value"],
                    producer_id=command.get("producer_id", "unknown"),
                )
                self._queue[msg.msg_id] = msg
                METRICS.gauge("queue_depth").set(len(self._queue))
                logger.debug(f"[{self.node_id}] APPLY enqueue msg_id={msg.msg_id}")

            elif op == "dequeue":
                msg_id = command["msg_id"]
                msg = self._queue.pop(msg_id, None)
                if msg:
                    msg.in_flight = True
                    msg.in_flight_at = time.time()
                    msg.delivery_count += 1
                    self._in_flight[msg_id] = msg
                    METRICS.gauge("queue_depth").set(len(self._queue))
                    logger.debug(f"[{self.node_id}] APPLY dequeue msg_id={msg_id}")

            elif op == "ack":
                msg_id = command["msg_id"]
                msg = self._in_flight.pop(msg_id, None)
                if msg:
                    logger.debug(f"[{self.node_id}] APPLY ack msg_id={msg_id}")

            elif op == "nack":
                msg_id = command["msg_id"]
                msg = self._in_flight.pop(msg_id, None)
                if msg:
                    msg.in_flight = False
                    msg.in_flight_at = None
                    # Re-enqueue at the front
                    new_queue = OrderedDict()
                    new_queue[msg_id] = msg
                    new_queue.update(self._queue)
                    self._queue = new_queue
                    logger.debug(f"[{self.node_id}] APPLY nack msg_id={msg_id} re-enqueued")

            elif op == "expire":
                msg_id = command["msg_id"]
                self._queue.pop(msg_id, None)
                self._in_flight.pop(msg_id, None)

    
    # Helpers

    def _pick_message(self, key_filter: Optional[str]) -> Optional[Message]:
        for msg in self._queue.values():
            if key_filter is None or msg.key == key_filter:
                return msg
        return None

    # Background tasks

    async def _redelivery_loop(self):
        """Re-enqueue in-flight messages that exceed ack_timeout (at-least-once)."""
        while True:
            await asyncio.sleep(5)
            now = time.time()
            async with self._lock:
                expired_ids = [
                    msg_id for msg_id, msg in self._in_flight.items()
                    if msg.in_flight_at and (now - msg.in_flight_at) > self.ack_timeout
                ]

            for msg_id in expired_ids:
                logger.warning(f"[{self.node_id}] Redelivering unacked message {msg_id}")
                METRICS.counter("queue_redeliveries_total").inc()
                await self.propose({"op": "nack", "msg_id": msg_id})

    async def _ttl_cleanup_loop(self):
        """Remove messages that exceed TTL."""
        while True:
            await asyncio.sleep(60)
            now = time.time()
            async with self._lock:
                expired = [
                    msg_id for msg_id, msg in self._queue.items()
                    if (now - msg.enqueued_at) > self.message_ttl
                ]
            for msg_id in expired:
                logger.info(f"[{self.node_id}] Message TTL expired: {msg_id}")
                await self.propose({"op": "expire", "msg_id": msg_id})