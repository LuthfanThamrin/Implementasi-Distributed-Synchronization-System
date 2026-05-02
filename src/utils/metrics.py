

import time
from collections import defaultdict
from typing import Dict


class Counter:
    def __init__(self):
        self._value = 0

    def inc(self, amount: int = 1):
        self._value += amount

    @property
    def value(self) -> int:
        return self._value


class Gauge:
    def __init__(self):
        self._value = 0.0

    def set(self, value: float):
        self._value = value

    def inc(self, amount: float = 1):
        self._value += amount

    def dec(self, amount: float = 1):
        self._value -= amount

    @property
    def value(self) -> float:
        return self._value


class Histogram:
    
    def __init__(self, buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0)):
        self.buckets = buckets
        self._counts: Dict[float, int] = {b: 0 for b in buckets}
        self._sum = 0.0
        self._count = 0

    def observe(self, value: float):
        self._sum += value
        self._count += 1
        for b in self.buckets:
            if value <= b:
                self._counts[b] += 1

    @property
    def mean(self) -> float:
        return self._sum / self._count if self._count else 0.0


class MetricsRegistry:
    """Node-level metrics registry."""
    def __init__(self):
        self.counters: Dict[str, Counter] = defaultdict(Counter)
        self.gauges: Dict[str, Gauge] = defaultdict(Gauge)
        self.histograms: Dict[str, Histogram] = defaultdict(Histogram)
        self._start_time = time.time()

    def counter(self, name: str) -> Counter:
        return self.counters[name]

    def gauge(self, name: str) -> Gauge:
        return self.gauges[name]

    def histogram(self, name: str) -> Histogram:
        return self.histograms[name]

    def export_text(self) -> str:
        """Export metrics in Prometheus text format."""
        lines = []
        lines.append(f"# uptime_seconds {time.time() - self._start_time:.2f}")
        for name, c in self.counters.items():
            lines.append(f"{name} {c.value}")
        for name, g in self.gauges.items():
            lines.append(f"{name} {g.value}")
        for name, h in self.histograms.items():
            lines.append(f"{name}_count {h._count}")
            lines.append(f"{name}_sum {h._sum:.6f}")
            lines.append(f"{name}_mean {h.mean:.6f}")
        return "\n".join(lines)

    def snapshot(self) -> dict:
        return {
            "uptime_seconds": round(time.time() - self._start_time, 2),
            "counters": {k: v.value for k, v in self.counters.items()},
            "gauges": {k: v.value for k, v in self.gauges.items()},
            "histograms": {
                k: {"count": v._count, "sum": v._sum, "mean": v.mean}
                for k, v in self.histograms.items()
            },
        }


# Global registry instance
METRICS = MetricsRegistry()