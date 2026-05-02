import asyncio
import logging
from typing import Any, Dict, List, Optional

import aiohttp

logger = logging.getLogger(__name__)


class MessageBus:
    

    def __init__(self, node_id: str, max_retries: int = 3, retry_delay: float = 0.1):
        self.node_id = node_id
        self.max_retries = max_retries
        self.retry_delay = retry_delay

    async def send(
        self,
        url: str,
        payload: Dict,
        timeout: float = 2.0,
        retries: Optional[int] = None,
    ) -> Optional[Dict]:
        
        attempts = retries if retries is not None else self.max_retries
        for attempt in range(attempts + 1):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        url,
                        json=payload,
                        timeout=aiohttp.ClientTimeout(total=timeout),
                    ) as resp:
                        return await resp.json()
            except aiohttp.ClientConnectorError:
                logger.debug(f"[{self.node_id}] Connection refused: {url} (attempt {attempt+1})")
            except asyncio.TimeoutError:
                logger.debug(f"[{self.node_id}] Timeout: {url} (attempt {attempt+1})")
            except Exception as e:
                logger.debug(f"[{self.node_id}] Send error: {e}")

            if attempt < attempts:
                await asyncio.sleep(self.retry_delay * (2 ** attempt))  # exponential backoff

        return None

    async def broadcast(
        self,
        peers: List[str],
        path: str,
        payload: Dict,
        timeout: float = 1.0,
    ) -> Dict[str, Optional[Dict]]:
        tasks = {
            peer: asyncio.create_task(self.send(f"http://{peer}{path}", payload, timeout=timeout))
            for peer in peers
        }
        results = {}
        for peer, task in tasks.items():
            try:
                results[peer] = await task
            except Exception:
                results[peer] = None
        return results