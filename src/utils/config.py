
import os
from dataclasses import dataclass, field
from typing import List


def _peers(env_var: str) -> List[str]:
    raw = os.getenv(env_var, "")
    return [p.strip() for p in raw.split(",") if p.strip()]


@dataclass
class RaftConfig:
    election_timeout_min: int = int(os.getenv("RAFT_ELECTION_TIMEOUT_MIN", 150))
    election_timeout_max: int = int(os.getenv("RAFT_ELECTION_TIMEOUT_MAX", 300))
    heartbeat_interval: float = float(os.getenv("RAFT_HEARTBEAT_INTERVAL", 50)) / 1000  # convert ms → s


@dataclass
class NodeConfig:
    node_id: str = os.getenv("NODE_ID", "node1")
    host: str = os.getenv("NODE_HOST", "0.0.0.0")
    port: int = int(os.getenv("NODE_PORT", 8000))

    lock_peers: List[str] = field(default_factory=lambda: _peers("LOCK_PEERS"))
    queue_peers: List[str] = field(default_factory=lambda: _peers("QUEUE_PEERS"))
    cache_peers: List[str] = field(default_factory=lambda: _peers("CACHE_PEERS"))

    # Lock
    lock_timeout: int = int(os.getenv("LOCK_TIMEOUT", 30))
    deadlock_detection_interval: int = int(os.getenv("DEADLOCK_DETECTION_INTERVAL", 5))

    # Queue
    queue_max_size: int = int(os.getenv("QUEUE_MAX_SIZE", 10000))
    message_ttl: int = int(os.getenv("MESSAGE_TTL", 3600))
    ack_timeout: int = int(os.getenv("ACK_TIMEOUT", 30))

    # Cache
    cache_max_size: int = int(os.getenv("CACHE_MAX_SIZE", 1000))
    cache_replacement_policy: str = os.getenv("CACHE_REPLACEMENT_POLICY", "LRU")
    cache_coherence_protocol: str = os.getenv("CACHE_COHERENCE_PROTOCOL", "MESI")

    # Raft
    raft: RaftConfig = field(default_factory=RaftConfig)

    # Logging
    log_level: str = os.getenv("LOG_LEVEL", "INFO")

    # Metrics
    metrics_port: int = int(os.getenv("METRICS_PORT", 9090))
    metrics_enabled: bool = os.getenv("METRICS_ENABLED", "true").lower() == "true"

    # Redis
    redis_host: str = os.getenv("REDIS_HOST", "localhost")
    redis_port: int = int(os.getenv("REDIS_PORT", 6379))
    redis_db: int = int(os.getenv("REDIS_DB", 0))


CONFIG = NodeConfig()