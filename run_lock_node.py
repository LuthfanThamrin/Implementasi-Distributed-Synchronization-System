#!/usr/bin/env python
"""Entry point for Lock Manager Node"""

import asyncio
import logging
import sys
import signal

from src.utils.config import CONFIG
from src.nodes.lock_manager import LockManagerNode

# Setup logging
logging.basicConfig(
    level=getattr(logging, CONFIG.log_level),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

shutdown_event = asyncio.Event()

def signal_handler(signum, frame):
    logger.info(f"Received signal {signum}, shutting down...")
    shutdown_event.set()

async def main():
    logger.info(f"Starting Lock Manager Node: {CONFIG.node_id}")
    
    node = LockManagerNode(
        node_id=CONFIG.node_id,
        host=CONFIG.host,
        port=CONFIG.port,
        peers=CONFIG.lock_peers,
        lock_timeout=CONFIG.lock_timeout,
        deadlock_check_interval=CONFIG.deadlock_detection_interval,
    )
    
    try:
        await node.start()
        logger.info(f"Lock Manager Node {CONFIG.node_id} is running")
        # Keep the node running until shutdown signal
        await shutdown_event.wait()
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
