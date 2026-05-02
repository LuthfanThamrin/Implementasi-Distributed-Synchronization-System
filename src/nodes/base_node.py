import asyncio
import json
import logging
import os
from typing import List, Optional

from aiohttp import web

from src.consensus.raft import RaftNode, Role
from src.utils.metrics import METRICS

logger = logging.getLogger(__name__)


class BaseNode:
    def __init__(
        self,
        node_id: str,
        host: str,
        port: int,
        peers: List[str],
        election_timeout_min: float = 0.150,
        election_timeout_max: float = 0.300,
        heartbeat_interval: float = 0.050,
    ):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.peers = peers

        self.raft = RaftNode(
            node_id=node_id,
            peers=peers,
            election_timeout_min=election_timeout_min,
            election_timeout_max=election_timeout_max,
            heartbeat_interval=heartbeat_interval,
            apply_callback=self.apply_command,
        )

        self.app = web.Application()
        self._setup_routes()

    def _setup_routes(self):
        """Register base routes. Subclasses call super() then add their own."""
        self.app.router.add_post("/raft/request_vote", self._handle_request_vote)
        self.app.router.add_post("/raft/append_entries", self._handle_append_entries)
        self.app.router.add_get("/health", self._handle_health)
        self.app.router.add_get("/metrics", self._handle_metrics)
        self.app.router.add_get("/status", self._handle_status)

   
    # Lifecycle
    

    async def start(self):
        await self.raft.start()
        runner = web.AppRunner(self.app)
        await runner.setup()
        site = web.TCPSite(runner, self.host, self.port)
        await site.start()
        logger.info(f"[{self.node_id}] HTTP server listening on {self.host}:{self.port}")

    # To be overridden by subclasses
  

    async def apply_command(self, command: dict):
        """Apply a committed log entry to the state machine. Override in subclasses."""
        pass

    # Raft RPC endpoints
   

    async def _handle_request_vote(self, request: web.Request) -> web.Response:
        body = await request.json()
        result = await self.raft.handle_request_vote(body)
        return web.json_response(result)

    async def _handle_append_entries(self, request: web.Request) -> web.Response:
        body = await request.json()
        result = await self.raft.handle_append_entries(body)
        return web.json_response(result)

   
    # Utility endpoints
  

    async def _handle_health(self, request: web.Request) -> web.Response:
        status = self.raft.status()
        return web.json_response({"status": "ok", **status})

    async def _handle_metrics(self, request: web.Request) -> web.Response:
        snap = METRICS.snapshot()
        snap["raft"] = self.raft.status()
        return web.Response(text=METRICS.export_text(), content_type="text/plain")

    async def _handle_status(self, request: web.Request) -> web.Response:
        return web.json_response(self.raft.status())

   
    # Helpers for subclasses
    

    def is_leader(self) -> bool:
        return self.raft.role == Role.LEADER

    async def propose(self, command: dict) -> bool:
        return await self.raft.propose(command)