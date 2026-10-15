# Vahak Failure Modes & Delivery Guarantees

This document details the exact delivery semantics, crash behavior, and fault boundaries of Vahak.

---

## 1. Core Guarantees

### At-Least-Once Delivery
In `ACK_MODE=durable` (default), Vahak guarantees **at-least-once delivery** for all acknowledged webhooks:
- If a client receives an HTTP `200 OK` (`{"status":"captured"}`), the webhook request and its corresponding delivery job are guaranteed to be committed to non-volatile storage (WAL / database) before the response is transmitted.
- Any crash after this point (`SIGKILL`, `kill -9`, container eviction, or kernel panic) will result in the recovery sweeper re-queueing and delivering the uncompleted job upon restart.

### No Strict Ordering Guarantees
Vahak does **not** guarantee strict FIFO ordering across different webhooks:
- In-flight deliveries are dispatched across a pool of concurrent worker goroutines.
- If a destination endpoint temporarily fails, that specific job undergoes exponential backoff and is rescheduled, while subsequent webhooks to healthy endpoints proceed immediately.
- Downstream consumers **must** be idempotent and should rely on event timestamps or payload sequence numbers rather than delivery order.

---

## 2. Pipeline Step Crash Analysis

The table below details system behavior if the process terminates abruptly (`kill -9`) at each stage of the lifecycle:

| Pipeline Stage | State at Crash | Client Outcome | Recovery Behavior on Restart |
|---|---|---|---|
| **Step 1: Network Ingress** | Request in TCP transit or kernel buffer | Client receives TCP connection reset / timeout | Client treats as transient network failure and retries. |
| **Step 2: Memory Ingest Channel** | Enqueued in `ingestCh`, uncommitted | **Durable Mode:** Connection severed; no 200 OK sent.<br>**Fast Mode:** Client already received 200 OK; in-memory buffer lost. | **Durable Mode:** Safe. Client knows to retry.<br>**Fast Mode:** Data loss possible across abrupt process termination. |
| **Step 3: Batch Storage Commit** | In-flight batch transaction | Atomic transaction rolls back; connection severed. | No orphaned rows. Storage remains consistent. |
| **Step 4: Post-Commit / Pre-Dispatch** | Row committed to `requests` & `delivery_jobs` (`status = 'pending'`); client received 200 OK | Client received successful acknowledgment | **Zero Loss:** On restart, startup sweeper queries `status = 'pending'` and delivers to target. |
| **Step 5: In-Flight HTTP Delivery** | Outbound TCP POST flying to destination URL | Client already acknowledged | Target may or may not have received event. Because job is still `pending` in DB, sweeper redelivers upon restart. (At-least-once). |
| **Step 6: Post-Delivery Status Flush** | Target received 200 OK; job ID buffered in `deliveredCh` | Client already acknowledged | If process dies before the 100ms status flush commits `status = 'delivered'`, the sweeper may re-send the event upon restart. (Downstream must be idempotent). |

---

## 3. Storage Durability Boundaries

### SQLite WAL Mode (`PRAGMA synchronous = NORMAL`)
- **Process Crash / `kill -9` / OS Panic:** Complete crash consistency. WAL frames committed prior to the ACK are preserved. Verified via automated chaos test (`TestCrashRecovery_ZeroLoss`).
- **Physical Power Loss (Unplugging Power):** Under `synchronous = NORMAL`, SQLite syncs WAL frames to disk during checkpoints rather than per-transaction. A sudden physical power failure or hardware reset can roll back transactions committed in the last checkpoint interval (~few hundred milliseconds).
- **Strict Power-Loss Requirement:** If physical power loss resilience is strictly required over throughput, configure `_pragma=synchronous(FULL)` or use an external replicated PostgreSQL cluster.

### Acknowledgment Modes (`ACK_MODE`)
- **`ACK_MODE=durable` (Default):** The HTTP handler blocks until the multi-row batch is committed to disk before sending HTTP 200 OK. Guarantees zero acknowledged-event loss across process crashes.
- **`ACK_MODE=fast` (Opt-in):** The HTTP handler sends HTTP 200 OK immediately upon channel enqueue (<0.5ms). Provides microsecond ingress latency for telemetry workloads where process-death memory loss is an acceptable trade-off.
