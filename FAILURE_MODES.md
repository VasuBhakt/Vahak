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
- **Process Crash (`kill -9`, SIGKILL, segfault, application crash):** Complete crash consistency. WAL frames committed prior to the 200 OK acknowledgment are preserved in the operating system buffer cache and flushed by the OS. Verified via automated chaos test (`TestCrashRecovery_ZeroLoss`).
- **Physical Power Loss / OS Crash:** Under `synchronous = NORMAL`, SQLite syncs WAL frames to physical non-volatile storage during checkpoints rather than per-transaction. A sudden physical power loss (unplugging the machine) or operating system kernel panic can roll back transactions committed in the last checkpoint interval.
- **Strict Power-Loss Requirement:** If physical power loss resilience is strictly required over throughput, configure `_pragma=synchronous(FULL)` or deploy Vahak with a replicated PostgreSQL cluster.

---

## 4. Duplicate Delivery Sources & Idempotency

Because Vahak enforces **at-least-once delivery**, duplicate deliveries can occur under two specific failure conditions:

1. **Crash Between Delivery and Status Flush:** The worker successfully transmits the webhook and receives a `200 OK` from the destination target, but the Vahak process terminates (`kill -9`) before the batched `status = 'delivered'` commit completes. Upon restart, the sweeper detects the job as `pending` and redelivers it.
2. **Lease Expiry on a Slow Target:** If an upstream destination hangs or takes longer to respond than the atomic worker lease (15 seconds), the sweeper will consider the delivery abandoned and re-claim the job for concurrent dispatch.

### Deduplication Contract
Every outbound webhook transmitted by Vahak includes standardized tracking headers:
- `X-Vahak-Event-ID`: The immutable UUID of the original captured webhook. This ID remains identical across all retries and redeliveries.
- `X-Vahak-Delivery-ID`: A unique UUID generated per individual HTTP attempt.
- `X-Vahak-Timestamp`: RFC3339 timestamp of the delivery dispatch.

> **Target Implementation:** Receivers should maintain an idempotency cache of recently processed `X-Vahak-Event-ID` values to discard duplicate deliveries safely.

---

## 5. Terminal States & Retention Invariants

- **Retry Schedule:** Jobs undergo exponential backoff with full randomized jitter across up to 5 attempts (`MaxAttempts = 5`), with backoff capping at 30 minutes.
- **Terminal States:** A delivery job reaches a terminal state when:
  1. The target responds with a `2xx` status code (`status = 'delivered'`).
  2. All 5 retry attempts are exhausted without success (`status = 'failed'`).
- **Retention Invariant:** The hourly retention cleaner (`RETENTION_DAYS`, default 7) executes:
  ```sql
  DELETE FROM requests 
  WHERE received_at < cutoff
    AND NOT EXISTS (
      SELECT 1 FROM delivery_jobs j 
      WHERE j.request_id = requests.id 
        AND j.status NOT IN ('delivered', 'failed', 'dead')
    );
  ```
  Even during prolonged downstream outages exceeding the retention window, **pending jobs are never cascaded away or deleted** until they have reached a terminal state.
