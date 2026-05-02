# Distributed Sync System

[MUHAMMAD LUTHFAN THAMRIN]
[11231058] 
Tugas Individu 2 — Parallel and Distributed System
Link video YouTube: [https://youtu.be/5NPUZ9ltcJo]

---

## Overview

Distributed Sync System adalah sistem terdistribusi berbasis **Raft Consensus Algorithm** yang terdiri dari tiga service utama:

1. **Lock Node** (Port 800x) → Distributed Lock Manager berbasis Raft dengan deadlock detection menggunakan Wait-For Graph
2. **Queue Node** (Port 810x) → Distributed Queue dengan mekanisme ACK/NACK dan at-least-once delivery
3. **Cache Node** (Port 820x) → Distributed Cache dengan protokol MESI (Modified, Exclusive, Shared, Invalid)

Ketiganya dikoordinasikan melalui leader election, log replication, dan heartbeat untuk memastikan konsistensi dan fault tolerance.

---

## Architecture

```
          ┌──────────────────┐
          │  Client Request  │
          └────────┬─────────┘
                   │
        ┌──────────▼──────────┐
        │   Git Bash / curl   │
        └──────────┬──────────┘
     ┌─────────────┼─────────────────────┐
     │             │                     │
┌────▼────────┐ ┌──▼──────────┐ ┌────────▼────────┐
│ Lock Nodes  │ │ Queue Nodes │ │  Cache Nodes    │
│ :8000–8002  │ │ :8100–8102  │ │  :8200–8202     │
│ Raft + Lock │ │ Raft + MQ   │ │  Raft + MESI    │
└─────────────┘ └─────────────┘ └─────────────────┘

## Build & Run

```bash
docker compose -f docker/docker-compose.yml up --build
```



## Step-by-Step Demo

### 1. Cek Health & Leader

```bash
curl http://localhost:8000/health
curl http://localhost:8001/health
curl http://localhost:8002/health

curl http://localhost:8100/health
curl http://localhost:8101/health
curl http://localhost:8102/health

curl http://localhost:8200/health
curl http://localhost:8201/health
curl http://localhost:8202/health
```

> `"role_code": 2` → node tersebut adalah **Leader**

---

### 2. Demo Lock Manager (Leader: port 8001)

Acquire exclusive lock:
```bash
curl -X POST http://localhost:8001/lock/acquire \
  -H "Content-Type: application/json" \
  -d '{"key":"database","mode":"X","client_id":"app-1"}'
```

Coba dari client lain — harus ditolak:
```bash
curl -X POST http://localhost:8001/lock/acquire \
  -H "Content-Type: application/json" \
  -d '{"key":"database","mode":"X","client_id":"app-2","wait":false}'
```

Acquire shared lock — dua client bisa bersamaan:
```bash
curl -X POST http://localhost:8001/lock/acquire \
  -H "Content-Type: application/json" \
  -d '{"key":"config","mode":"S","client_id":"reader-1"}'

curl -X POST http://localhost:8001/lock/acquire \
  -H "Content-Type: application/json" \
  -d '{"key":"config","mode":"S","client_id":"reader-2"}'
```

Lihat semua lock aktif:
```bash
curl http://localhost:8001/lock/status
```

Release lock:
```bash
curl -X POST http://localhost:8001/lock/release \
  -H "Content-Type: application/json" \
  -d '{"key":"database","client_id":"app-1"}'
```

---

### 3. Demo Queue System (Leader: port 8101)

Enqueue pesan:
```bash
curl -X POST http://localhost:8101/queue/enq \
  -H "Content-Type: application/json" \
  -d '{"key":"orders","value":{"order_id":1,"item":"laptop"},"producer_id":"shop"}'

curl -X POST http://localhost:8101/queue/enq \
  -H "Content-Type: application/json" \
  -d '{"key":"orders","value":{"order_id":2,"item":"phone"},"producer_id":"shop"}'
```

Cek status queue:
```bash
curl http://localhost:8101/queue/status
```

Dequeue — catat msg_id dari output:
```bash
curl -X POST http://localhost:8101/queue/deq \
  -H "Content-Type: application/json" \
  -d '{"consumer_id":"worker-1"}'
```

ACK — pesan selesai diproses:
```bash
curl -X POST http://localhost:8101/queue/ack \
  -H "Content-Type: application/json" \
  -d '{"msg_id":"<MSG_ID_DARI_DEQ>"}'
```

NACK — pesan gagal, kembalikan ke queue:
```bash
curl -X POST http://localhost:8101/queue/nack \
  -H "Content-Type: application/json" \
  -d '{"msg_id":"<MSG_ID_DARI_DEQ>"}'
```

---

### 4. Demo Cache MESI (Leader: port 8202)

PUT data:
```bash
curl -X POST http://localhost:8202/cache/put \
  -H "Content-Type: application/json" \
  -d '{"key":"user:1","val":{"name":"Budi","score":95}}'
```

GET data:
```bash
curl -X POST http://localhost:8202/cache/get \
  -H "Content-Type: application/json" \
  -d '{"key":"user:1"}'
```

Cek MESI state di semua node:
```bash
curl http://localhost:8202/cache/status
curl http://localhost:8200/cache/status
curl http://localhost:8201/cache/status
```

---

### 5. Demo Fault Tolerance

Matikan satu node:
```bash
docker stop lock-node1
```

Sistem masih berjalan dengan 2/3 node:
```bash
curl -X POST http://localhost:8001/lock/acquire \
  -H "Content-Type: application/json" \
  -d '{"key":"test","mode":"X","client_id":"c1"}'
```

Hidupkan kembali — node rejoin otomatis:
```bash
docker start lock-node1
curl http://localhost:8000/health
```

---

## Metrics Monitoring

```bash
curl http://localhost:8001/metrics
curl http://localhost:8101/metrics
curl http://localhost:8202/metrics
```

Contoh output:
```
lock_acquires_total 10
lock_releases_total 8
queue_enq_total 5
queue_deq_total 4
cache_hits_total 3
cache_misses_total 1
```

---

## Performance Benchmark

```bash
python benchmarks/load_test_scenarios.py \
  --host localhost \
  --lock-port 8001 \
  --queue-port 8101 \
  --cache-port 8202
```

