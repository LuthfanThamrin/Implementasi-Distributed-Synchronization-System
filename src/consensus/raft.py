import asyncio
import json
import logging
import random
import time
from dataclasses import dataclass, field, asdict
from enum import IntEnum
from typing import Any, Dict, List, Optional, Callable, Awaitable

import aiohttp

logger = logging.getLogger(__name__)


class Role(IntEnum):
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 2


@dataclass
class LogEntry:
    term: int
    index: int
    command: Dict[str, Any]


@dataclass
class RaftState:
    # Persistent state
    current_term: int = 0
    voted_for: Optional[str] = None
    log: List[LogEntry] = field(default_factory=list)

    # Volatile state
    commit_index: int = -1
    last_applied: int = -1

    # Leader state (reinitialized after election)
    next_index: Dict[str, int] = field(default_factory=dict)
    match_index: Dict[str, int] = field(default_factory=dict)


class RaftNode:
   

    def __init__(
        self,
        node_id: str,
        peers: List[str],
        election_timeout_min: float = 0.150,
        election_timeout_max: float = 0.300,
        heartbeat_interval: float = 0.050,
        apply_callback: Optional[Callable[[Dict], Awaitable[None]]] = None,
    ):
        self.node_id = node_id
        self.peers = peers  # list of "host:port"
        self.election_timeout_min = election_timeout_min
        self.election_timeout_max = election_timeout_max
        self.heartbeat_interval = heartbeat_interval
        self.apply_callback = apply_callback

        self.state = RaftState()
        self.role = Role.FOLLOWER
        self.leader_id: Optional[str] = None

        self._election_timer_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._last_heartbeat = time.monotonic()
        self._lock = asyncio.Lock()

        # Metrics
        self._elections_started = 0
        self._elections_won = 0
        self._heartbeats_sent = 0
        self._entries_replicated = 0

   
    # Public API
    
    async def start(self):
        """Start the Raft node background tasks."""
        logger.info(f"[{self.node_id}] Starting Raft node, peers={self.peers}")
        self._election_timer_task = asyncio.create_task(self._election_timer_loop())

    async def stop(self):
        if self._election_timer_task:
            self._election_timer_task.cancel()
        if self._heartbeat_task:
            self._heartbeat_task.cancel()

    async def propose(self, command: Dict[str, Any]) -> bool:
        """
        Propose a new command to the cluster.
        Only leaders can accept proposals.
        Returns True if committed by majority.
        """
        if self.role != Role.LEADER:
            return False

        async with self._lock:
            index = len(self.state.log)
            entry = LogEntry(term=self.state.current_term, index=index, command=command)
            self.state.log.append(entry)
            logger.info(f"[{self.node_id}] Proposed entry index={index}")

        # Replicate immediately
        logger.info(f"[{self.node_id}] Starting replication for index={index}")
        success = await self._replicate_log()
        logger.info(f"[{self.node_id}] Replication result: {success}")
        return success

    def status(self) -> Dict:
        return {
            "node_id": self.node_id,
            "role": self.role.name,
            "role_code": int(self.role),
            "term": self.state.current_term,
            "leader_id": self.leader_id,
            "log_length": len(self.state.log),
            "commit_index": self.state.commit_index,
            "peers": self.peers,
            "metrics": {
                "elections_started": self._elections_started,
                "elections_won": self._elections_won,
                "heartbeats_sent": self._heartbeats_sent,
                "entries_replicated": self._entries_replicated,
            }
        }

   
    # RPC Handlers (called by HTTP server)
   

    async def handle_request_vote(self, body: Dict) -> Dict:
        """Handle RequestVote RPC from a candidate."""
        async with self._lock:
            candidate_term = body["term"]
            candidate_id = body["candidate_id"]
            candidate_log_index = body.get("last_log_index", -1)
            candidate_log_term = body.get("last_log_term", -1)

            # If we see a higher term, revert to follower
            if candidate_term > self.state.current_term:
                await self._become_follower(candidate_term)

            vote_granted = False
            if (
                candidate_term >= self.state.current_term
                and (self.state.voted_for is None or self.state.voted_for == candidate_id)
                and self._is_log_up_to_date(candidate_log_index, candidate_log_term)
            ):
                self.state.voted_for = candidate_id
                vote_granted = True
                self._reset_election_timeout()
                logger.info(f"[{self.node_id}] Voted for {candidate_id} in term {candidate_term}")

            return {"term": self.state.current_term, "vote_granted": vote_granted}

    async def handle_append_entries(self, body: Dict) -> Dict:
      
        async with self._lock:
            leader_term = body["term"]
            leader_id = body["leader_id"]
            prev_log_index = body.get("prev_log_index", -1)
            prev_log_term = body.get("prev_log_term", -1)
            entries_raw = body.get("entries", [])
            leader_commit = body.get("leader_commit", -1)

            if leader_term < self.state.current_term:
                return {"term": self.state.current_term, "success": False}

            # Valid leader contact — reset election timer
            if leader_term > self.state.current_term:
                await self._become_follower(leader_term)

            self.leader_id = leader_id
            self._reset_election_timeout()
            if self.role != Role.FOLLOWER:
                self.role = Role.FOLLOWER

            # Check log consistency
            if prev_log_index >= 0:
                if len(self.state.log) <= prev_log_index:
                    return {"term": self.state.current_term, "success": False}
                if self.state.log[prev_log_index].term != prev_log_term:
                    # Delete conflicting entry and all that follow
                    self.state.log = self.state.log[:prev_log_index]
                    return {"term": self.state.current_term, "success": False}

            # Append new entries
            for e in entries_raw:
                entry = LogEntry(term=e["term"], index=e["index"], command=e["command"])
                idx = entry.index
                if idx < len(self.state.log):
                    if self.state.log[idx].term != entry.term:
                        self.state.log = self.state.log[:idx]
                        self.state.log.append(entry)
                else:
                    self.state.log.append(entry)

            # Update commit index
            if leader_commit > self.state.commit_index:
                self.state.commit_index = min(leader_commit, len(self.state.log) - 1)
                await self._apply_committed_entries()

            return {"term": self.state.current_term, "success": True}

    # Internal Election Logic
   

    async def _election_timer_loop(self):
        """Background task: fires election when timeout elapses without heartbeat."""
        while True:
            timeout = random.uniform(self.election_timeout_min, self.election_timeout_max)
            await asyncio.sleep(timeout)
            if self.role != Role.LEADER:
                elapsed = time.monotonic() - self._last_heartbeat
                if elapsed >= timeout:
                    await self._start_election()

    def _reset_election_timeout(self):
        self._last_heartbeat = time.monotonic()

    async def _start_election(self):
        async with self._lock:
            self.role = Role.CANDIDATE
            self.state.current_term += 1
            self.state.voted_for = self.node_id
            self._elections_started += 1
            term = self.state.current_term
            logger.info(f"[{self.node_id}] Starting election for term {term}")

        votes = 1  # Vote for self
        total_nodes = len(self.peers) + 1
        majority = total_nodes // 2 + 1

        last_log_index = len(self.state.log) - 1
        last_log_term = self.state.log[last_log_index].term if self.state.log else -1

        body = {
            "term": term,
            "candidate_id": self.node_id,
            "last_log_index": last_log_index,
            "last_log_term": last_log_term,
        }

        tasks = [self._send_request_vote(peer, body) for peer in self.peers]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, dict) and result.get("vote_granted"):
                votes += 1
            elif isinstance(result, dict) and result.get("term", 0) > self.state.current_term:
                await self._become_follower(result["term"])
                return

        if self.role == Role.CANDIDATE and votes >= majority:
            await self._become_leader()
        else:
            async with self._lock:
                if self.role == Role.CANDIDATE:
                    self.role = Role.FOLLOWER

    async def _become_leader(self):
        async with self._lock:
            self.role = Role.LEADER
            self.leader_id = self.node_id
            self._elections_won += 1
            # Initialize leader state
            next_idx = len(self.state.log)
            self.state.next_index = {p: next_idx for p in self.peers}
            self.state.match_index = {p: -1 for p in self.peers}
            logger.info(f"[{self.node_id}] Became LEADER for term {self.state.current_term}")

        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def _become_follower(self, term: int):
        self.role = Role.FOLLOWER
        self.state.current_term = term
        self.state.voted_for = None
        self._reset_election_timeout()
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            self._heartbeat_task = None

    async def _heartbeat_loop(self):
       
        while self.role == Role.LEADER:
            await self._replicate_log()
            self._heartbeats_sent += 1
            await asyncio.sleep(self.heartbeat_interval)

    async def _replicate_log(self) -> bool:
        
        if self.role != Role.LEADER:
            return False

        tasks = [self._send_append_entries(peer) for peer in self.peers]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        acks = 1  # Leader itself
        for result in results:
            if isinstance(result, dict) and result.get("success"):
                acks += 1
            elif isinstance(result, dict):
                term = result.get("term", 0)
                if term > self.state.current_term:
                    await self._become_follower(term)
                    return False

        total = len(self.peers) + 1
        majority = total // 2 + 1
        logger.info(f"[{self.node_id}] Replication: acks={acks}/{total}, majority={majority}, peers={self.peers}")
        if acks >= majority:
            new_commit = len(self.state.log) - 1
            if new_commit > self.state.commit_index:
                self.state.commit_index = new_commit
                self._entries_replicated += 1
                logger.info(f"[{self.node_id}] Committing entries up to index={new_commit}")
                await self._apply_committed_entries()
            return True
        return False

    async def _apply_committed_entries(self):
        """Apply all committed but not yet applied log entries to state machine."""
        while self.state.last_applied < self.state.commit_index:
            self.state.last_applied += 1
            entry = self.state.log[self.state.last_applied]
            if self.apply_callback:
                try:
                    await self.apply_callback(entry.command)
                except Exception as e:
                    logger.error(f"[{self.node_id}] Apply callback error: {e}")

   
    # Network helpers
   

    async def _send_request_vote(self, peer: str, body: Dict) -> Optional[Dict]:
        url = f"http://{peer}/raft/request_vote"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=body, timeout=aiohttp.ClientTimeout(total=0.5)) as resp:
                    return await resp.json()
        except Exception:
            return None

    async def _send_append_entries(self, peer: str) -> Optional[Dict]:
        prev_index = self.state.next_index.get(peer, len(self.state.log)) - 1
        prev_term = self.state.log[prev_index].term if prev_index >= 0 and self.state.log else -1
        entries = [
            asdict(e) for e in self.state.log[prev_index + 1:]
        ]
        body = {
            "term": self.state.current_term,
            "leader_id": self.node_id,
            "prev_log_index": prev_index,
            "prev_log_term": prev_term,
            "entries": entries,
            "leader_commit": self.state.commit_index,
        }
        url = f"http://{peer}/raft/append_entries"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=body, timeout=aiohttp.ClientTimeout(total=0.5)) as resp:
                    result = await resp.json()
                    if result.get("success"):
                        self.state.next_index[peer] = len(self.state.log)
                        self.state.match_index[peer] = len(self.state.log) - 1
                    else:
                        # Decrement next_index and retry next heartbeat
                        self.state.next_index[peer] = max(0, self.state.next_index.get(peer, 1) - 1)
                    return result
        except Exception:
            return None

  
    # Helpers
    

    def _is_log_up_to_date(self, candidate_last_index: int, candidate_last_term: int) -> bool:
        if not self.state.log:
            return True
        my_last = self.state.log[-1]
        if candidate_last_term != my_last.term:
            return candidate_last_term > my_last.term
        return candidate_last_index >= my_last.index