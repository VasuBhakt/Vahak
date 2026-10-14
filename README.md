# Vahak

Vahak is a high-throughput, single-node webhook ingestion and delivery engine written in Go. It provides at-least-once delivery guarantees, in-process JavaScript payload transformation, and dual-backend persistence (embedded SQLite WAL or PostgreSQL) without external message brokers.

---

<<<<<<< HEAD
## Technical Architecture
=======
## 🏗️ Technical Architecture
>>>>>>> bdb8c68 (docs: overhaul architecture, benchmarks, and dual-engine testing guide)

```
[ HTTP Ingest ]
      │
      ▼
[ Ingest Channel ]  ── (cap: 100,000, non-blocking enqueue, < 0.5ms ACK)
      │
      ├──────────────────────────────────────────────┐
      │ (async batching)                             │ (fast-path delivery)
      ▼                                              ▼
[ Batch Flusher ]                              [ Worker Pool (100 goroutines) ]
      │                                              │
      │ 50-500 row multi-insert chunks               ├─► [ Goja JS VM (50ms timeout) ]
      ▼                                              │
[ Storage Engine ]                                   ▼
(SQLite WAL / PostgreSQL)                     [ Circuit Breaker ]
                                                     │
                                       ┌─────────────┴─────────────┐
                                       ▼ (2xx OK)                  ▼ (5xx / Timeout / Circuit Open)
                                 [ Complete ]               [ Save to delivery_jobs ]
                                                                   │
                                                                   ▼
                                                            [ Retry Sweeper ]
                                                            (30s interval, exponential backoff)
```

### 1. Ingestion Pipeline
- **Fast-Path Enqueue:** The HTTP handler decodes the payload, validates the endpoint ID, enqueues to an in-memory buffered channel (`chan *store.IngestItem`, cap: 100,000), and immediately returns `200 OK` (`{"status":"captured"}`) in `< 0.5ms`.
- **Channel Backpressure:** If ingestion rate exceeds storage write bandwidth and the 100,000-slot buffer fills, the enqueue operation blocks, naturally applying TCP backpressure to upstream webhook senders rather than dropping data or causing out-of-memory (OOM) crashes.
- **Multi-Row Chunked Flushing:** A dedicated goroutine drains `ingestCh` and batches records into parameter chunks (up to 500 rows per transaction). This reduces database roundtrips by 50x compared to single-row inserts.

### 2. Lock-Free Delivery Mechanics
- **Fast-Path Bypass:** Outbound HTTP delivery is dispatched immediately from memory. Successful deliveries (`2xx`) never write to the `delivery_jobs` table.
- **Failure-Only Queueing:** If delivery fails (network partition, 5xx, timeout, or circuit breaker open), the job is committed to `delivery_jobs` with status `pending`, initial `attempts = 1`, and `next_attempt` calculated via exponential backoff. This eliminates 99% of table-lock contention under normal operating conditions.
- **Indexed Sweeper:** A background ticker sweeps `delivery_jobs` every 30 seconds with bounded pagination (`LIMIT 100`) using a partial index (`WHERE status = 'pending'`).

### 3. Graceful Drain & Shutdown Lifecycle
On receiving `SIGINT` or `SIGTERM`:
1. `http.Server.Shutdown(ctx)` stops accepting new connections and waits for active in-flight requests to complete and enqueue.
2. Background context is cancelled to stop the ingestion loop.
3. `batchWorker` drains remaining items in `ingestCh` and executes a final batch flush to disk before releasing the database lock.
4. Storage engine closes cleanly via `sync.WaitGroup`.

---

<<<<<<< HEAD
## Storage Engine Implementations
=======
## 💾 Storage Engine Implementations
>>>>>>> bdb8c68 (docs: overhaul architecture, benchmarks, and dual-engine testing guide)

Vahak abstracts storage behind a unified Go interface (`store.Storage`), selectable via `DB_ENGINE`:

| Parameter | Embedded SQLite (`DB_ENGINE=sqlite`) | PostgreSQL (`DB_ENGINE=postgres`) |
|---|---|---|
| **Driver** | `modernc.org/sqlite` (Pure Go, `CGO_ENABLED=0`) | `jackc/pgx/v5` (`pgxpool`) |
| **Concurrency Model** | Multi-reader single-writer (WAL mode) | Multi-connection pool (`max_conns=200`) |
| **Durability Pragma** | `PRAGMA synchronous(NORMAL)` | PostgreSQL WAL defaults |
| **Write Protocol** | Multi-row parameter batching (`INSERT INTO ... VALUES (?,?), (?,?)...`) | Multi-row batching / `COPY` protocol |
| **Locking Overhead** | Zero network latency; writer serialized via WAL | TCP roundtrip per transaction |
| **Pruning Engine** | Hourly `DELETE ... WHERE received_at < NOW() - INTERVAL` | Same |

### SQLite Pragmas
```sql
PRAGMA busy_timeout = 5000;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA wal_autocheckpoint = 10000;
PRAGMA foreign_keys = ON;
PRAGMA cache_size = -64000;       -- 64MB memory cache
PRAGMA temp_store = MEMORY;
PRAGMA mmap_size = 268435456;     -- 256MB memory-mapped I/O
```

---

<<<<<<< HEAD
## Testing Environments: Local vs Docker
=======
## 🧪 Testing Environments: Local vs Docker
>>>>>>> bdb8c68 (docs: overhaul architecture, benchmarks, and dual-engine testing guide)

Understanding the difference between the two runtime modes is essential for accurate benchmarking and production deployment.

### 1. Local Testing (Host + Embedded SQLite WAL)

Runs directly on the host machine using embedded SQLite. Eliminates all container and network virtualization overhead.

- **Storage:** Local file (`vahak.db`) in WAL mode.
- **Network Path:** `Client -> Localhost Kernel Socket -> Vahak (Go runtime) -> In-Memory Channel -> Disk (WAL)`.
- **Observed Throughput:** **8,500 – 10,200 RPS** at `-c 200`.
- **Windows Networking Constraint:** On Windows, avoid targeting `localhost` (which triggers IPv6/IPv4 dual-stack resolution delays). Always benchmark against `127.0.0.1`. Keep client concurrency at `c <= 200` to prevent Winsock ephemeral port exhaustion (`connectex refused`).

#### How to Run Locally:
```bash
# 1. Configure .env for SQLite
DB_ENGINE=sqlite
DB_PATH=vahak.db
PORT=8080
API_KEY=docker_test_key
ALLOW_LOCAL_TARGETS=true

# 2. Run
go run ./cmd/server

# 3. Create Endpoint
curl -X POST http://127.0.0.1:8080/api/endpoints \
  -H "X-API-Key: docker_test_key" \
  -H "Content-Type: application/json" \
  -d '{"name":"local-bench","target_url":"http://127.0.0.1:9090"}'

# 4. Benchmark
hey -n 15000 -c 200 -m POST \
  -d '{"event":"bench"}' \
  -T "application/json" \
  "http://127.0.0.1:8080/hooks/<ENDPOINT_UUID>"
```

---

### 2. Docker Testing (Containerized + PostgreSQL + Sink)

Runs the full distributed architecture across isolated Docker containers:
- `db`: PostgreSQL 16 Alpine (`max_connections=250`)
- `vahak`: Compiled static Go binary running against the `db` container
- `sink`: Mock receiver (`tests/sink/main.go`) listening on `:9090`

- **Storage:** PostgreSQL over Docker bridge network.
- **Network Path:** `Client -> Docker Proxy -> Vahak Container -> TCP Socket -> PostgreSQL Container`.
- **Observed Throughput:** **4,000 – 7,000 RPS** at `-c 200`.
- **Windows Docker Desktop Constraint:** On Windows, Docker runs inside a WSL2 VM. Requests forwarded through `127.0.0.1:8080` pass through `docker-proxy.exe`. At high concurrency (`-c 500`), the Windows userspace TCP proxy listen backlog (128) saturates, triggering `dial tcp 127.0.0.1:8080: connectex: No connection could be made because the target machine actively refused it`.
- **Accurate High-Concurrency Docker Benchmarking:** To benchmark above 200 concurrency without hitting the Windows host TCP proxy limit, execute `hey` directly **inside** the Docker bridge network:

```bash
# Run hey inside the Docker network (bypasses Windows docker-proxy)
docker run --rm --network vahak_default williamyeh/hey \
  -n 15000 -c 500 -m POST \
  -d '{"event":"docker.bench"}' \
  -T "application/json" \
  "http://vahak:8080/hooks/<ENDPOINT_UUID>"
```

#### How to Run Docker Stack:
```bash
# Start Postgres, Vahak, and Sink
docker compose -f docker-compose.yml -f docker-compose.test.yml up --build

# In a separate terminal, register an endpoint pointing to the sink container
curl -X POST http://127.0.0.1:8080/api/endpoints \
  -H "X-API-Key: docker_test_key" \
  -H "Content-Type: application/json" \
  -d '{"name":"docker-test","target_url":"http://sink:9090"}'

# Trigger webhook
curl -X POST http://127.0.0.1:8080/hooks/<ENDPOINT_UUID> \
  -H "Content-Type: application/json" \
  -d '{"event":"docker.test"}'
```

---

<<<<<<< HEAD
## Benchmark Comparison
=======
## 📊 Benchmark Comparison
>>>>>>> bdb8c68 (docs: overhaul architecture, benchmarks, and dual-engine testing guide)

Tested with 15,000 requests per run:

| Metric | Embedded SQLite (Local) | PostgreSQL 16 (Docker) |
|---|---|---|
| **Concurrency** | 200 connections | 200 connections |
| **Throughput** | **9,016 – 10,215 RPS** | **4,000 – 6,888 RPS** |
| **Average Latency** | 19.2 ms – 21.7 ms | 26.9 ms – 46.0 ms |
| **P99 Latency** | 39.5 ms – 40.8 ms | 134.9 ms – 1.26 s |
| **Network Latency to DB** | 0 ms (In-process memory/disk) | Container bridge TCP socket |
| **Status Codes** | 100% `200 OK` (15,000 / 15,000) | 100% `200 OK` (15,000 / 15,000) |

---

<<<<<<< HEAD
## Fault Tolerance & Resilience
=======
## 🛡️ Fault Tolerance & Resilience
>>>>>>> bdb8c68 (docs: overhaul architecture, benchmarks, and dual-engine testing guide)

- **SSRF Mitigation:** Outbound deliveries are validated against private IP ranges (`10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`, `127.0.0.0/8`, loopback). Blocked unless `ALLOW_LOCAL_TARGETS=true`.
- **CPU Starvation Protection:** User-defined JavaScript transformations running in the embedded `goja` runtime are wrapped with a `time.AfterFunc` watchdog terminating execution after 50ms to prevent `while(true)` denial-of-service.
- **Circuit Breaker State Machine:** Tracks failure states per target host. 5 consecutive delivery failures trip the circuit breaker into `Open` state. Requests during cooldown are rescheduled to `delivery_jobs` without firing outbound TCP calls. Cooldown scales exponentially (30s, 60s, 120s, up to 5m) before moving to `Half-Open` for single-probe verification.
- **Bounded Retention:** Background worker executes hourly pruning of records where `received_at < NOW() - INTERVAL 'RETENTION_DAYS'` to enforce fixed storage ceilings.
<<<<<<< HEAD
=======

---

## ⚙️ Configuration

| Environment Variable | Description | Default |
|---|---|---|
| `PORT` | HTTP server listen port | `8080` |
| `DB_ENGINE` | Persistence driver: `sqlite` or `postgres` | `sqlite` |
| `DB_PATH` | Path to SQLite file (when `DB_ENGINE=sqlite`) | `vahak.db` |
| `DB_URL` | PostgreSQL connection string | `""` |
| `DB_POOL_URL` | PostgreSQL connection pool URL (falls back to `DB_URL`) | `""` |
| `API_KEY` | Bearer token for protected `/api/*` endpoints | `docker_test_key` |
| `ALLOW_LOCAL_TARGETS` | Permit delivery to private/loopback IPs | `false` |
| `RETENTION_DAYS` | Storage TTL in days for requests table | `7` |

---

## 🔬 Test Suite

```bash
# Unit & integration tests
go test -v ./...

# Race condition detection
go test -race ./...
```

---
>>>>>>> bdb8c68 (docs: overhaul architecture, benchmarks, and dual-engine testing guide)

---

<<<<<<< HEAD
## Configuration

| Environment Variable | Description | Default |
|---|---|---|
| `PORT` | HTTP server listen port | `8080` |
| `DB_ENGINE` | Persistence driver: `sqlite` or `postgres` | `sqlite` |
| `DB_PATH` | Path to SQLite file (when `DB_ENGINE=sqlite`) | `vahak.db` |
| `DB_URL` | PostgreSQL connection string | `""` |
| `DB_POOL_URL` | PostgreSQL connection pool URL (falls back to `DB_URL`) | `""` |
| `API_KEY` | Bearer token for protected `/api/*` endpoints | `docker_test_key` |
| `ALLOW_LOCAL_TARGETS` | Permit delivery to private/loopback IPs | `false` |
| `RETENTION_DAYS` | Storage TTL in days for requests table | `7` |

---

## Test Suite

```bash
# Unit & integration tests
go test -v ./...

# Race condition detection
go test -race ./...
```

---

## License

MIT License. See [LICENSE.txt](LICENSE.txt).
=======
The name **Vahak** (*वाहक*) is derived from Sanskrit, meaning *Carrier*, *Bearer*, or *Messenger* — historically referring to one who reliably delivers a message across distances without faltering.

---

## 🤝 Contributing

Contributions, issues, and feature requests are welcome!

1. Fork the repository
2. Create your feature branch (`git checkout -b feat/your-feature`)
3. Commit your changes (`git commit -m 'feat: add some feature'`)
4. Push to the branch (`git push origin feat/your-feature`)
5. Open a Pull Request

**Maintainer**: [Swastik Bose (VasuBhakt)](https://github.com/VasuBhakt)

---

## 📜 License

Distributed under the MIT License. See [LICENSE.txt](LICENSE.txt) for details.
>>>>>>> bdb8c68 (docs: overhaul architecture, benchmarks, and dual-engine testing guide)
