#!/usr/bin/env python3
import asyncio, aiohttp, time, sys
from collections import deque


class LeaderResolver:
    def __init__(self, host, lock_ports, queue_ports, cache_ports):
        self.host = host
        self.ports = {
            "lock":  lock_ports,
            "queue": queue_ports,
            "cache": cache_ports,
        }
        self.leader_url = {
            "lock":  f"http://{host}:{lock_ports[0]}",
            "queue": f"http://{host}:{queue_ports[0]}",
            "cache": f"http://{host}:{cache_ports[0]}",
        }

    async def discover(self, session, verbose=False):
        for svc, ports in self.ports.items():
            for port in ports:
                try:
                    async with session.get(
                        f"http://{self.host}:{port}/health",
                        timeout=aiohttp.ClientTimeout(total=2)
                    ) as r:
                        if r.status == 200:
                            data = await r.json()
                            if data.get("role") == "LEADER":
                                new_url = f"http://{self.host}:{port}"
                                if new_url != self.leader_url[svc]:
                                    print(f"  [{svc}] leader berubah -> port {port} ({data.get('node_id')})")
                                    self.leader_url[svc] = new_url
                                elif verbose:
                                    print(f"  [{svc}] leader -> port {port} ({data.get('node_id')})")
                                break
                except Exception:
                    pass

    def redirect_from_response(self, svc, data):
        leader_id = data.get("leader_id", "")
        if not leader_id:
            return False
        try:
            node_num = int(leader_id.split("-")[-1])
            new_port = self.ports[svc][node_num - 1]
            new_url = f"http://{self.host}:{new_port}"
            if new_url != self.leader_url[svc]:
                print(f"  [{svc}] redirect 409 -> {leader_id} (port {new_port})")
                self.leader_url[svc] = new_url
            return True
        except (ValueError, IndexError):
            return False

    def get(self, svc):
        return self.leader_url[svc]


class Monitor:
    def __init__(self, host, lock_ports, queue_ports, cache_ports):
        self.resolver = LeaderResolver(host, lock_ports, queue_ports, cache_ports)
        self.session  = None

        self.lock_ops  = 0; self.lock_lat  = deque(maxlen=200)
        self.queue_ops = 0; self.queue_lat = deque(maxlen=200)
        self.cache_ops = 0; self.cache_lat = deque(maxlen=200)

        self._lock_last  = 0
        self._queue_last = 0
        self._cache_last = 0
        self._tick = time.monotonic()

    async def connect(self):
        self.session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=50),
            timeout=aiohttp.ClientTimeout(total=3)
        )
        print("Mencari leader di setiap cluster...")
        await self.resolver.discover(self.session, verbose=True)
        print()

    async def close(self):
        if self.session:
            await self.session.close()

    async def _post(self, svc, path, **kwargs):
        for attempt in range(3):
            url = self.resolver.get(svc) + path
            try:
                async with self.session.post(url, **kwargs) as r:
                    try:
                        data = await r.json()
                    except Exception:
                        data = {}
                    if r.status == 409:
                        if not self.resolver.redirect_from_response(svc, data):
                            await self.resolver.discover(self.session)
                        continue
                    return r.status, data
            except asyncio.TimeoutError:
                await self.resolver.discover(self.session)
            except Exception:
                pass
        return None, {}

    async def lock_worker(self):
        i = 0
        while True:
            key, cid = f"key-{i % 10}", f"client-{i % 5}"
            t = time.monotonic()
            status, _ = await self._post(
                "lock", "/lock/acquire",
                json={"key": key, "mode": "X", "client_id": cid, "wait": False}
            )
            self.lock_lat.append(time.monotonic() - t)
            if status == 200:
                self.lock_ops += 1
                await self._post("lock", "/lock/release",
                                 json={"key": key, "client_id": cid})
            await asyncio.sleep(0.05)
            i += 1

    async def queue_worker(self):
        i = 0
        while True:
            t = time.monotonic()
            status, _ = await self._post(
                "queue", "/queue/enq",
                json={"message": f"msg-{i}"}
            )
            self.queue_lat.append(time.monotonic() - t)
            if status == 200:
                self.queue_ops += 1
            await asyncio.sleep(0.05)
            i += 1

    async def cache_worker(self):
        i = 0
        while True:
            key = f"key-{i % 50}"
            t = time.monotonic()
            if i % 5 < 4:
                status, _ = await self._post("cache", "/cache/get", json={"key": key})
            else:
                status, _ = await self._post(
                    "cache", "/cache/put",
                    json={"key": key, "value": f"val-{i}"}
                )
            self.cache_lat.append(time.monotonic() - t)
            if status == 200:
                self.cache_ops += 1
            await asyncio.sleep(0.05)
            i += 1

    async def _rediscover_loop(self, every_sec=3):
        while True:
            await asyncio.sleep(every_sec)
            await self.resolver.discover(self.session)

    def avg_ms(self, d):
        return sum(d) / len(d) * 1000 if d else 0

    async def display_loop(self, interval=10):
        tick = 0
        while True:
            await asyncio.sleep(interval)
            tick += 1

            now = time.monotonic()
            dt  = now - self._tick
            self._tick = now

            lock_tps  = (self.lock_ops  - self._lock_last)  / dt
            queue_tps = (self.queue_ops - self._queue_last) / dt
            cache_tps = (self.cache_ops - self._cache_last) / dt
            self._lock_last  = self.lock_ops
            self._queue_last = self.queue_ops
            self._cache_last = self.cache_ops

            lport = self.resolver.get("lock").split(":")[-1]
            qport = self.resolver.get("queue").split(":")[-1]
            cport = self.resolver.get("cache").split(":")[-1]

            print(f"\n[tick {tick} | +{interval}s]")
            print(f"lock  leader :{lport}  |  {lock_tps:.0f} ops/s  |  avg {self.avg_ms(self.lock_lat):.1f} ms")
            print(f"queue leader :{qport}  |  {queue_tps:.0f} req/s  |  avg {self.avg_ms(self.queue_lat):.1f} ms")
            print(f"cache leader :{cport}  |  {cache_tps:.0f} req/s  |  avg {self.avg_ms(self.cache_lat):.1f} ms")

    async def run(self, interval=10):
        await self.connect()
        print(f"Monitor jalan, update tiap {interval} detik... (Ctrl+C stop)\n")
        try:
            await asyncio.gather(
                self.display_loop(interval),
                self._rediscover_loop(every_sec=3),
                *[self.lock_worker()  for _ in range(5)],
                *[self.queue_worker() for _ in range(3)],
                *[self.cache_worker() for _ in range(5)],
            )
        except (KeyboardInterrupt, asyncio.CancelledError):
            pass
        finally:
            await self.close()


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--host",       default="localhost")
    p.add_argument("--lock-port",  type=int, default=8000)
    p.add_argument("--queue-port", type=int, default=8101)
    p.add_argument("--cache-port", type=int, default=8202)
    p.add_argument("--interval",   type=int, default=10)
    a = p.parse_args()

    def port_range(base):
        b = (base // 100) * 100
        return [b, b+1, b+2]

    try:
        asyncio.run(Monitor(
            a.host,
            port_range(a.lock_port),
            port_range(a.queue_port),
            port_range(a.cache_port),
        ).run(interval=a.interval))
    except KeyboardInterrupt:
        pass