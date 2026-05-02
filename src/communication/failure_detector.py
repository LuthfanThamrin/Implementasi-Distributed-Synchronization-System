

import asyncio
import logging
import math
import time
from collections import deque
from typing import Dict, List, Optional

import aiohttp

logger = logging.getLogger(__name__)


class HeartbeatWindow:
    def __init__(self, window_size: int = 200):
        self._intervals: deque = deque(maxlen=window_size)
        self._last_received: Optional[float] = None

    def record(self):
        now = time.monotonic()
        if self._last_received is not None:
            interval = now - self._last_received
            self._intervals.append(interval)
        self._last_received = now

    @property
    def mean(self) -> float:
        if not self._intervals:
            return 1.0
        return sum(self._intervals) / len(self._intervals)

    @property
    def variance(self) -> float:
        if len(self._intervals) < 2:
            return 0.0
        m = self.mean
        return sum((x - m) ** 2 for x in self._intervals) / len(self._intervals)

    @property
    def std(self) -> float:
        return math.sqrt(self.variance) or 0.001


class FailureDetector:
    def __init__(
        self,
        node_id: str,
        peers: List[str],
        probe_interval: float = 1.0,
        phi_threshold: float = 8.0,
    ):
        self.node_id = node_id
        self.peers = peers
        self.probe_interval = probe_interval
        self.phi_threshold = phi_threshold

        self._windows: Dict[str, HeartbeatWindow] = {p: HeartbeatWindow() for p in peers}
        self._last_probe: Dict[str, float] = {}
        self._suspected: Dict[str, bool] = {p: False for p in peers}

    async def start(self):
        asyncio.create_task(self._probe_loop())

    def phi(self, peer: str) -> float:
        """Compute phi suspicion level for a peer."""
        window = self._windows.get(peer)
        if not window or not window._last_received:
            return 0.0
        elapsed = time.monotonic() - window._last_received
        mean = window.mean
        std = window.std
        # CDF of exponential distribution approximation
        x = (elapsed - mean) / std if std > 0 else 0
        phi_val = -math.log10(1 - self._normal_cdf(x))
        return phi_val

    def is_suspected(self, peer: str) -> bool:
        return self.phi(peer) >= self.phi_threshold

    def status(self) -> dict:
        return {
            peer: {
                "phi": round(self.phi(peer), 3),
                "suspected": self.is_suspected(peer),
                "mean_interval": round(self._windows[peer].mean, 4),
            }
            for peer in self.peers
        }

    async def _probe_loop(self):
        while True:
            tasks = [self._probe(peer) for peer in self.peers]
            await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.sleep(self.probe_interval)

    async def _probe(self, peer: str):
        url = f"http://{peer}/health"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=1.0)) as resp:
                    if resp.status == 200:
                        self._windows[peer].record()
                        if self._suspected[peer]:
                            logger.info(f"[{self.node_id}] Peer {peer} recovered.")
                            self._suspected[peer] = False
        except Exception:
            if not self._suspected[peer] and self.is_suspected(peer):
                self._suspected[peer] = True
                logger.warning(f"[{self.node_id}] Peer {peer} SUSPECTED (phi={self.phi(peer):.2f})")

    @staticmethod
    def _normal_cdf(x: float) -> float:
        return (1.0 + math.erf(x / math.sqrt(2))) / 2