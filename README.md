# 🦅 Vahak

Vahak is a durable, single-node webhook ingestion and delivery engine written in Go. It provides crash-tested at-least-once delivery for acknowledged events across process crashes (`kill -9`), featuring dual-engine storage (embedded SQLite WAL or PostgreSQL), in-process JavaScript transformations, and adaptive circuit breaking without external message brokers.

---

## 🏗️ Technical Architecture

```
[ HTTP Ingest ]
      │
      ▼
[ Ingest Channel ] ── (Bounded queue, capacity: 20,000)
      │
      ▼
[ Batch Flusher ]  ── Aggregates 50-500 requests into atomic multi-row transactions
      │
      ▼
[ Storage Commit ] ── Atomically inserts `requests` AND `delivery_jobs` (status = 'pending')
      │
      ├──────────────────────────────────────────────┐
      ▼                                              ▼
[ 200 OK to Client ]                           [ Delivery Worker Pool ]
(Only after durable commit in durable mode)          │
                                                     ├─► [ Goja JS VM (50ms watchdog) ]
                                                     │
                                                     ▼
                                              [ Circuit Breaker ]
                                                     │
                                       ┌─────────────┴─────────────┐
                                       ▼ (2xx OK)                  ▼ (5xx / Timeout / Circuit Open)
                           [ Batch Status Update ]        [ Reschedule in DB with Backoff ]
                           (status = 'delivered')                  │
                                                                   ▼
                                                            [ Recovery Sweeper ]
                                                            (Runs on startup + 2s ticker, leased drain loop)
```

### 1. Ingestion & Acknowledgment Modes
Vahak supports two acknowledgment modes via `ACK_MODE`:
- **`ACK_MODE=durable` (Default, Recommended):** The HTTP handler enqueues to the batcher, blocks until the atomic batch transaction commits to durable storage, and only then acknowledges `200 OK` to the sender and dispatches to the delivery worker pool. Guarantees **zero acknowledged-event loss** across abrupt process crashes (`kill -9`).
- **`ACK_MODE=fast` (Opt-in):** Returns `200 OK` immediately upon channel enqueue (<0.5ms). Dispatches delivery concurrently with background batch writing. Designed for high-volume non-critical telemetry where in-memory loss across an abrupt process kill is acceptable.

### 2. At-Least-Once Delivery Mechanics
- **Pre-Dispatch Commitment:** Before an event is dispatched to downstream targets, both the payload (`requests`) and its queue entry (`delivery_jobs`, `status = 'pending'`) are committed to storage atomically.
- **Delivery Confirmation:** Successful deliveries push the job ID to `deliveredCh`, where a status worker periodically updates `delivery_jobs SET status = 'delivered' WHERE id IN (...)` to minimize transaction lock frequency.
- **Recovery Sweeper & Leased Drain Loop:** On startup and every 2 seconds thereafter, a background sweeper claims pending/expired jobs in batches of 100 using atomic worker leases (`ClaimPendingJobs` with a 15-second lease). It continuously loops until the backlog is fully drained. Any webhook in-flight during an abrupt crash is safely recovered after lease expiry and re-delivered upon restart.
- **Terminal State Machine:** Deliveries that encounter repeated failures are retried with exponential backoff up to 5 attempts (`MaxAttempts = 5`). Upon exhausting retries, the job transitions to a terminal `failed` state.
- **Retention Protection Invariant:** Storage retention pruning (`PruneExpired`) runs hourly to enforce `RETENTION_DAYS`, but strictly protects uncompleted deliveries via `WHERE NOT EXISTS (SELECT 1 FROM delivery_jobs j WHERE j.request_id = requests.id AND j.status NOT IN ('delivered', 'failed', 'dead'))`. Webhooks destined for temporarily offline targets are never pruned until completed or retry exhaustion.
- For complete failure mode specifications, see [FAILURE_MODES.md](FAILURE_MODES.md).

### 3. Graceful Drain Lifecycle
On receiving `SIGINT` or `SIGTERM`:
1. `http.Server.Shutdown(ctx)` stops accepting new connections and waits for active in-flight requests to complete.
2. Background context cancels to halt new ingestion tickers.
3. `batchWorker` drains remaining items in `ingestCh` and commits a final batch to disk.
4. Storage engine closes cleanly via `sync.WaitGroup`.

---

## 💾 Storage Engine Implementations

Vahak abstracts persistence behind a unified Go interface (`store.Storage`), configurable via `DB_ENGINE`:

| Parameter | Embedded SQLite (`DB_ENGINE=sqlite`) | PostgreSQL (`DB_ENGINE=postgres`) |
|---|---|---|
| **Driver** | `modernc.org/sqlite` (Pure Go, `CGO_ENABLED=0`) | `jackc/pgx/v5` (`pgxpool`, `max_conns=200`) |
| **Concurrency Model** | Multi-reader, single-writer (WAL mode) | Multi-connection pool with simple query protocol |
| **Durability Pragma** | `PRAGMA synchronous(NORMAL)` (or `FULL` via `SQLITE_SYNCHRONOUS`) | PostgreSQL WAL defaults |
| **Write Protocol** | Multi-row batch parameterization | Multi-row batching / `COPY` protocol |
| **Data Retention** | Automated hourly pruning via `RETENTION_DAYS` | Automated hourly pruning via `RETENTION_DAYS` |

### SQLite Durability Pragma Details
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
> **Note on `synchronous=NORMAL`:** Under SQLite WAL mode, `NORMAL` guarantees zero database corruption and zero data loss across process crashes (`kill -9`, SIGKILL, segfault, application panic). In the event of catastrophic physical power loss (unplugging hardware) or an operating system kernel crash, uncheckpointed WAL frames in the OS page cache can be lost. If physical power-loss protection is strictly required, set `SQLITE_SYNCHRONOUS=FULL`. See [FAILURE_MODES.md](FAILURE_MODES.md) for details.

---

## 🚀 Deployment & Operational Modes

Vahak is designed around two primary operational patterns:

### Mode 1: Pure Local (Zero-Dependency SQLite)
*Best for: Local development, CI testing, edge gateways, single-node deployments.*

Runs directly on your host machine as a standalone single binary with zero external dependencies:
- **Database:** Embedded SQLite (`vahak.db`) running with pure-Go (`CGO_ENABLED=0`) and WAL mode.
- **Setup:**
  ```bash
  # .env configuration
  DB_ENGINE=sqlite
  DB_PATH=vahak.db
  ACK_MODE=durable          # or "fast" for maximum raw throughput
  API_KEY=your_secure_api_key
  ALLOW_LOCAL_TARGETS=true

  go run ./cmd/server/main.go
  ```

---

### Mode 2: Pure Docker Stack (PostgreSQL Cluster)
*Best for: Production servers, cloud deployments (EC2, Droplets, Kubernetes).*

Runs both Vahak and tuned PostgreSQL 16 containers in an isolated internal Docker network (`vahak_default`), bypassing host OS socket bottlenecks:
- **Database:** PostgreSQL 16 Alpine with connection pooling (`pgxpool`, `max_connections=250`) and streaming `COPY` protocol micro-batching.
- **Setup:**
  ```bash
  # Start the complete stack
  docker compose up -d

  # View live logs
  docker compose logs -f vahak
  ```

> ⚠️ **Note on Hybrid Setups (Local Host Binary + Docker Postgres):**
> Running the host binary while pointing `DB_URL` to a containerized Postgres on Windows often encounters host-OS friction (IPv6 `[::1]` resolution conflicts with native Windows services, and bash shell environment shadowing). Use **Mode 1** for local development and **Mode 2** for containerized/production environments.

---

## 🧪 Benchmarking & Verification

### In-Container Benchmark (Docker Network)
Benchmark from inside the Docker bridge network to measure pure daemon throughput without host network proxy limits:
```bash
docker run --rm --network vahak_default williamyeh/hey \
  -n 15000 -c 200 -m POST \
  -d '{"event":"bench"}' \
  -T "application/json" \
  "http://vahak:8080/hooks/<ENDPOINT_UUID>"
```

---

## 📊 Measured Benchmarks

Evaluated with `hey` (-n 15000, 200 concurrent connections) across the supported deployment targets:

| Mode | Engine & Execution Environment | Measured Throughput | Avg Latency | P99 Latency | Durability Guarantee |
|---|---|---|---|---|---|
| **`ACK_MODE=durable`** (Default) | Embedded SQLite (WAL) — Host Native (Windows NVMe) | **2,200 – 3,400 RPS** | 58.0 – 85.0 ms | 190.0 – 480.0 ms | **Crash-Tested Zero Loss:** Process-crash safe; committed to WAL before 200 OK (uncheckpointed frames subject to OS power loss without `SQLITE_SYNCHRONOUS=FULL`) |
| **`ACK_MODE=durable`** | PostgreSQL 16 — Internal Docker Network (`db:5432`) | **8,500 – 10,200 RPS** | 42.0 – 58.0 ms | 168.0 ms | **Crash-Tested Zero Loss:** `CopyFrom` committed before 200 OK |
| **`ACK_MODE=fast`** (Opt-in) | Embedded SQLite (WAL) — Host Native (Windows NVMe) | **40,000 – 55,000 RPS** | 3.2 – 4.5 ms | 23.0 – 35.0 ms | **In-Memory ACK:** Channel enqueue before commit |

---

## 💥 Chaos Crash Recovery Verification (`kill -9`)

Vahak includes an automated chaos test suite ([`tests/chaos/crash_test.go`](tests/chaos/crash_test.go)) executed on every CI build:

1. Starts an HTTP sink recording unique event IDs (`X-Vahak-Event-ID`).
2. Boots Vahak in `ACK_MODE=durable` and floods concurrent webhooks.
3. Mid-load, abruptly executes `kill -9` (`SIGKILL`) on the Vahak process at a **randomized threshold**.
4. Restarts Vahak with the same database, allowing atomic worker leases to expire.
5. Verifies that **100% of acknowledged requests** (`200 OK`) are recovered by the startup sweeper and delivered to the sink at least once.
6. Repeats across multiple randomized crash-recovery iterations and logs cumulative duplicate delivery metrics (configurable via `CHAOS_ITERATIONS`).

```bash
# Run the multi-iteration kill -9 chaos test locally
go test -v -timeout 180s ./tests/chaos/...
```

---

## 🛡️ Security & Fault Tolerance

- **SSRF Mitigation:** Custom dialer performs DNS resolution at dial time and blocks private subnets (RFC 1918), loopback, cloud instance metadata (`169.254.0.0/16`), and IPv6 link-local/unique-local ranges (`fe80::/10`, `fc00::/7`). IPv4-mapped IPv6 addresses are normalized before validation, and HTTP redirects are strictly disabled to prevent SSRF redirect-hopping. Permitted only when `ALLOW_LOCAL_TARGETS=true`.
- **API Key Startup Enforcement:** Refuses to boot if `API_KEY` is empty or set to the default `docker_test_key` without explicit `ALLOW_INSECURE_DEFAULT_KEY=true`.
- **CPU Starvation Protection:** User-defined JavaScript transformations running in the embedded `goja` runtime are wrapped with a `time.AfterFunc` watchdog terminating execution after 50ms to prevent infinite loops (`while(true)`).
- **Circuit Breaker State Machine:** Per-host failure tracking. 5 consecutive delivery failures trip the circuit into `Open` state. Requests during cooldown are rescheduled to `delivery_jobs` with exponential backoff without firing outbound network calls.
- **Bounded Retention:** Background worker executes hourly pruning of records where `received_at < NOW() - INTERVAL 'RETENTION_DAYS'`, with an explicit invariant protecting pending/retrying jobs from deletion during prolonged downstream outages.

---

## ⚙️ Configuration Reference

| Environment Variable | Description | Default |
|---|---|---|
| `PORT` | HTTP server listen port | `8080` |
| `ACK_MODE` | Ingestion acknowledgment mode: `durable` or `fast` | `durable` |
| `DB_ENGINE` | Storage driver: `sqlite` or `postgres` | `sqlite` |
| `DB_PATH` | Path to SQLite file (when `DB_ENGINE=sqlite`) | `vahak.db` |
| `SQLITE_SYNCHRONOUS` | SQLite sync pragma: `NORMAL` (process-crash safe) or `FULL` (power-loss safe) | `NORMAL` |
| `DB_URL` | PostgreSQL connection string | `""` |
| `DB_POOL_URL` | PostgreSQL connection pool URL (falls back to `DB_URL`) | `""` |
| `API_KEY` | Secret token for administrative `/api/*` endpoints (Required) | `""` |
| `ALLOW_INSECURE_DEFAULT_KEY` | Allow using default test keys in local dev/testing | `false` |
| `ALLOW_LOCAL_TARGETS` | Permit delivery to private/loopback IPs | `false` |
| `RETENTION_DAYS` | Storage TTL in days for requests table | `7` |

---

## 🔬 Test Suite

```bash
# Unit & integration tests
go test -v ./...

# Race condition detection
go test -race ./...

# Chaos crash recovery verification
go test -v -timeout 180s ./tests/chaos/...
```

---

## 🤓 Fun Fact

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
