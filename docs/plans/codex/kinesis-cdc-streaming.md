# Kinesis CDC Streaming (Postgres Logical Replication)

## Summary
Build a production-grade Python service (deployed on an existing AWS EKS cluster) that connects to an Amazon RDS for PostgreSQL Multi-AZ cluster endpoint, consumes a logical replication slot (wal2json), and forwards each change event to an Amazon Kinesis Data Stream with bounded-latency micro-batching. The service must not drop the replication slot and must only acknowledge/flush LSNs after the corresponding records are durably written to Kinesis.

## Goals / Success Criteria
- Correctness: do not ACK/flush any LSN until Kinesis confirms success for the corresponding event(s).
- Low latency: minimize added client-side latency; bound batching latency (default: 10ms).
- Scalability: handle bursts with bounded memory and backpressure.
- HA in EKS: run with `replicas >= 2` in active/standby; exactly one pod consumes a given slot at a time.
- Operational safety: robust reconnect behavior; no slot drops; no unbounded WAL growth from a stuck consumer.

## Non-goals (Avoid)
- Schema evolution support.
- Alerting integrations.
- Data transformation (beyond minimal parsing for partition keys / transport encoding).

## Key Decisions
- Python + psycopg3: use psycopg3 with asyncio (first-class async).
- Psycopg3 replication: use `AsyncCursor.copy("START_REPLICATION ...")` (COPY BOTH) and parse replication protocol frames manually (psycopg 3.3.2 does not expose a high-level logical replication consumer API).
- Replication slot: never drop; create-if-missing only.
- State: ACK/flush LSNs only after Kinesis write success.
- Config: use `pydantic-settings` for configuration.
- AWS auth: use existing EKS IRSA role for Kinesis access (no static AWS credentials in service config).
- Kinesis publishing: use `PutRecords` with micro-batching (bounded delay).
- Kinesis payload: publish raw wal2json JSON bytes (no envelope transformation).
- Retry policy: infinite retry of failed `PutRecords` entries; never ACK failed or unknown LSNs.
- Leader lock key: derive a deterministic 64-bit advisory lock key from `REPLICATION_SLOT` (`slot_hash64`) with optional explicit override.
- Partition key: prefer primary key when available; fall back without heavy parsing if needed.

## Scaling: "One Slot, One Consumer"
Postgres logical replication allows only **one active consumer per replication slot**.

This means:
- You *do not* run multiple consumers against the same slot.
- You scale downstream by writing once to Kinesis and having many Kinesis consumers (fanout).
- If you truly need multiple CDC consumers, you introduce **multiple independent slots** (often paired with multiple publications / table groups), and run one consumer per slot.

When to consider multiple slots/consumers:
- You need fanout to multiple downstream destinations with independent SLAs (separate slots let pipelines progress independently).
- You need isolation by data domain (split tables across slots/publications).
- A single consumer cannot keep up and replication lag grows (WAL retained bytes keeps increasing).

Given the stated volume (largest table ~37,000 events / 24h, ~0.43 events/sec average), a single slot + single consumer is expected to be more than sufficient.

## HA Strategy (Active/Standby in EKS)
Run the deployment with at least 2 replicas. Ensure only one pod consumes the slot by using **Postgres advisory locks**:
- All pods start and try to acquire the same advisory lock.
- Advisory lock key is computed as deterministic `slot_hash64(REPLICATION_SLOT)` unless `LEADER_LOCK_KEY_OVERRIDE` is set.
- The pod holding the lock is the leader and runs replication + publishing.
- Standby pods wait and retry periodically.
- If the leader dies or loses DB session, the lock is released automatically and a standby takes over.

This matches the “2 replicas with standby takeover” requirement while remaining simple and avoiding extra Kubernetes coordination dependencies.

## Architecture

### Components
1. **Leader election**
   - Holds a Postgres advisory lock for the lifetime of leadership on a dedicated DB session.
   - Uses deterministic `slot_hash64(REPLICATION_SLOT)` for lock acquisition unless an explicit override is configured.
2. **Slot ensure (admin)**
   - Checks if the slot exists; creates it if missing.
3. **Replication reader**
   - Starts logical replication from the slot using wal2json.
   - Reads messages and enqueues them to a bounded queue with backpressure.
4. **Kinesis publisher**
   - Drains the queue, micro-batches, publishes using `PutRecords`.
   - Retries only failed records indefinitely until success.
5. **ACK/feedback tracker**
   - Advances the “highest contiguous published LSN”.
   - Sends replication feedback/standby status updates to flush progress.
6. **Logging**
   - Structured logs to stdout (no alerting).

### Data Flow
1. Acquire advisory lock.
2. Ensure replication slot exists (create if missing).
3. Start replication stream.
4. For each wal2json message:
   - Compute `partition_key`.
   - Enqueue `(lsn, payload_bytes_raw_json, partition_key)` to a bounded queue.
5. Publisher micro-batches and calls `PutRecords`.
6. After Kinesis confirms success:
   - Mark the message’s LSN as published.
   - Advance contiguous ACK frontier.
7. Send replication feedback to flush the slot up to the ACK frontier.

## Service Location
Create a new service directory:
- `services/cdc-logical-replication/`

Contents:
- `src/` with the async application entrypoint.
- `tests/` with unit tests for ACK tracking and partition key extraction.
- `pyproject.toml` for dependencies.

## Configuration (Pydantic Settings)

### Postgres
- `PGHOST=<PGHOST>`
- `PGPORT=<PGPORT>`
- `PGUSER=<PGUSER>`
- `PGPASSWORD=<PGPASSWORD>`
- `PGDATABASE=<PGDATABASE>`
- `REPLICATION_SLOT=<REPLICATION_SLOT>` (default if unset: `etl_slot_wal2json`)
- `OUTPUT_PLUGIN=wal2json` (fixed)
- `CONNECT_TIMEOUT_S` (default: 5)

### wal2json options
Configure the decoding options (defaults match the research notes):
- `WAL2JSON_FORMAT_VERSION=2`
- `WAL2JSON_INCLUDE_TIMESTAMP=true`
- `WAL2JSON_INCLUDE_LSN=true`
- `WAL2JSON_INCLUDE_TRANSACTIONS=false`
- `WAL2JSON_INCLUDE_PK=true`

### Kinesis
- `AWS_REGION=<AWS_REGION>`
- `KINESIS_STREAM=<KINESIS_STREAM>`
- Batching:
  - `KINESIS_BATCH_MAX_RECORDS` (default: 200; max 500)
  - `KINESIS_BATCH_MAX_BYTES` (default: 900000; keep below 1MB API limit)
  - `KINESIS_BATCH_MAX_DELAY_MS` (default: 10)

### Partition Key
- `PARTITION_KEY_MODE=primary_key|fallback` (default: `primary_key`)
- `PARTITION_KEY_FALLBACK=lsn|table|static` (default: `lsn`)
- `PARTITION_KEY_STATIC_VALUE` (only if fallback is `static`)

### Backpressure
- `INFLIGHT_MAX_MESSAGES` (default: 10000)
- `INFLIGHT_MAX_BYTES` (default: 134217728)  # 128MB

### HA
- `LEADER_LOCK_KEY_DERIVATION=slot_hash64` (default)
- `LEADER_LOCK_KEY_OVERRIDE` (optional bigint; takes precedence over derivation)
- `STANDBY_RETRY_INTERVAL_S` (default: 5)

### Required Placeholders
- `<PGHOST>`
- `<PGPORT>`
- `<PGUSER>`
- `<PGPASSWORD>`
- `<PGDATABASE>`
- `<REPLICATION_SLOT>`
- `<AWS_REGION>`
- `<KINESIS_STREAM>`

## Replication Slot Management (No Drops)
On leadership acquisition:
1. Check if `REPLICATION_SLOT` exists in `pg_replication_slots`.
2. If missing, create with:
   - `SELECT * FROM pg_create_logical_replication_slot($slot, 'wal2json');`
3. Never call `pg_drop_replication_slot` or equivalent.

## Consuming the Replication Stream (Low Latency, psycopg 3.3.2)

This implementation targets `psycopg==3.3.2` and uses COPY BOTH mode. Psycopg 3.3.2 does not expose a public, high-level "logical replication cursor" API, so we treat replication as a byte stream and parse the standard logical replication protocol messages.

### Connection + COPY BOTH
- Connect with psycopg3 asyncio:
  - `psycopg.AsyncConnection.connect(..., autocommit=True, replication="database")`
- Start replication using COPY BOTH via:
  - `async with (await conn.cursor()).copy("START_REPLICATION SLOT ... LOGICAL ...") as copy:`
- Stream reads:
  - `buf = await copy.read()` returns raw bytes from the server.
- Feedback writes (ACK):
  - `await copy.write(feedback_bytes)` sends messages back to the server.

### START_REPLICATION statement
Use:
- `START_REPLICATION SLOT <slot> LOGICAL <start_lsn> (<wal2json_options>)`

Defaults:
- `start_lsn = 0/0` (server resumes from slot confirmed flush).
- wal2json options (from requirements):
  - `format-version = 2`
  - `include-timestamp = 1`
  - `include-lsn = 1`
  - `include-transactions = 0`
  - `include-pk = 1`

### Replication protocol parsing (minimal, fast)
Each message begins with a 1-byte type tag.

We only need:
- `b"w"` XLogData (contains the WAL positions + the logical payload)
- `b"k"` Primary keepalive (contains WAL end + reply request flag)

Unknown replication message tags: **fatal** (raise and restart via the outer reconnect loop). This avoids silently skipping protocol frames and corrupting ACK state.

Binary layouts (big-endian integers):
- XLogData (`b"w"`):
  - `wal_start` int64
  - `wal_end` int64
  - `server_time_us` int64
  - `payload` bytes (wal2json JSON)
- Keepalive (`b"k"`):
  - `wal_end` int64
  - `server_time_us` int64
  - `reply_requested` uint8

Implementation notes:
- Treat `wal_end` from XLogData as the authoritative LSN for ACK frontier tracking (this avoids parsing JSON for LSN).
- wal2json payload may include a trailing newline; strip it before JSON parsing for partition keys.

### Feedback / ACK message (StandbyStatusUpdate)
Only send feedback after Kinesis confirms publish success up to an LSN frontier.

Message type `b"r"` followed by:
- `write_lsn` int64
- `flush_lsn` int64
- `apply_lsn` int64
- `client_time_us` int64
- `reply_requested` uint8 (0 or 1)

Rules:
- `write_lsn = flush_lsn = apply_lsn = ack_frontier_lsn`
- Send feedback immediately when `ack_frontier_lsn` advances.
- Also send a heartbeat feedback at a fixed interval (e.g. 1s) to keep the slot progress fresh.
- If a keepalive arrives with `reply_requested=1`, respond promptly with feedback (using the latest safe `ack_frontier_lsn`).

### Replication protocol structs (paste-ready)

```python
import struct
import time

XLOGDATA_HDR = struct.Struct("!cqqq")
KEEPALIVE = struct.Struct("!cqqB")
STANDBY_STATUS = struct.Struct("!cqqqqB")

def now_us() -> int:
    return int(time.time() * 1_000_000)

def parse_xlogdata(buf: bytes) -> tuple[int, int, int, bytes]:
    tag, wal_start, wal_end, server_time_us = XLOGDATA_HDR.unpack_from(buf, 0)
    assert tag == b"w"
    payload = buf[XLOGDATA_HDR.size:]
    return wal_start, wal_end, server_time_us, payload

def parse_keepalive(buf: bytes) -> tuple[int, int, int]:
    tag, wal_end, server_time_us, reply_requested = KEEPALIVE.unpack(buf)
    assert tag == b"k"
    return wal_end, server_time_us, reply_requested

def build_standby_status(ack_lsn: int, *, reply_requested: int = 0) -> bytes:
    return STANDBY_STATUS.pack(
        b"r",
        ack_lsn,
        ack_lsn,
        ack_lsn,
        now_us(),
        1 if reply_requested else 0,
    )

def lsn_str_to_int(lsn: str) -> int:
    a, b = lsn.split("/")
    return (int(a, 16) << 32) | int(b, 16)

def lsn_int_to_str(lsn: int) -> str:
    return f"{(lsn >> 32):X}/{(lsn & 0xFFFFFFFF):X}"
```

### Feedback/ACK Rules (correctness)
- Never ACK/flush beyond the highest contiguous LSN whose events have been confirmed written to Kinesis.
- If `PutRecords` partially fails, do not advance the frontier past the first failed event LSN.
- On reconnect, restart replication and continue; the slot will resume from its confirmed flush position.

## Kinesis Publishing (Micro-batched PutRecords)
Publisher loop:
1. Collect up to `KINESIS_BATCH_MAX_RECORDS` or `KINESIS_BATCH_MAX_BYTES`, waiting up to `KINESIS_BATCH_MAX_DELAY_MS`.
2. Call `PutRecords`.
3. If partial failures occur, retry only the failed records indefinitely with exponential backoff + jitter.
4. Only mark records as published when the corresponding entry succeeds.
5. Keep ACK frontier pinned at the first missing contiguous success.

Implementation note:
- Boto3 is blocking: call `PutRecords` via `asyncio.to_thread()` (or a dedicated bounded thread executor) so replication reads/feedback remain responsive.

### PartitionKey: Primary Key vs Lowest Latency
Recommended default: `PartitionKey = primary key`.
- Benefits: stable routing and per-entity ordering when Kinesis has multiple shards.
- Cost: minimal JSON parsing. At the stated volume, this is not a latency bottleneck.

If you want to avoid parsing entirely (or PK is missing/unreliable):
- Use fallback `PartitionKey = lsn` (default fallback) or `static`.
- Keep the mode configurable; on parse failure, always fall back.

## Delivery Semantics
This design provides **at-least-once** delivery:
- If the process crashes after Kinesis success but before replication feedback flush, events may be replayed from the slot.
- Downstream consumers should be idempotent if duplicates matter.
- Extended Kinesis outage can increase retained WAL because ACK remains blocked by design until publish succeeds.

## Failure Modes & Recovery
- RDS writer failover: replication connection drops; reconnect to the cluster endpoint and continue.
- Kinesis throttling/transient errors: retry failed records indefinitely; do not ACK until success.
- Backpressure: if publisher lags, reader blocks on the bounded queue (prevents unbounded memory use).
- Leadership loss: if advisory lock is lost, stop consuming and return to standby loop.

## Testing
Unit tests should cover:
- LSN ordering and contiguous ACK frontier advancement.
- Partial failure handling (ACK does not advance past failed records).
- Partition key extraction + fallback behavior.
- Micro-batching boundaries (records/bytes/time).
- Backpressure behavior (bounded queues).

## Acceptance Scenarios
- Slot exists: service starts and consumes without attempting any drop.
- Slot missing: service creates slot once and consumes.
- Kinesis partial failures: retries succeed; ACK/flush only after success.
- Prolonged Kinesis outage: retries continue, and ACK/flush does not advance beyond the first failed LSN.
- Leader crash: standby becomes leader and resumes from last confirmed flush.

## Rollout
1. Deploy in a staging EKS namespace pointing at a staging RDS cluster and Kinesis stream.
2. Validate replication lag remains stable and WAL retained bytes does not grow unbounded.
3. Enable production with conservative batching (10ms delay) and monitor lag; tune if needed.
