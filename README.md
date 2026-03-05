# From MQTT to SQL: A Practical Guide to Sensor Data Ingestion

A tutorial demo showing why batching is critical for MQTT-to-SQL pipelines and how to implement time-window batching with TimescaleDB.


## What You'll Learn

### The Problem: Per-Message Inserts

The "per-message" approach in `per-message-insert.py` inserts each MQTT message individually:

- ❌ Connection overhead per insert
- ❌ Transaction overhead per insert
- ❌ WAL pressure compounds under load
- ❌ Latency grows as message rate increases

### The Solution: Time-Window Batching

The batched approach in `per-message-insert.py` buffers messages and flushes every N seconds:

- ✅ Predictable write cadence
- ✅ Bulk writes via the PostgreSQL COPY protocol
- ✅ Thread-safe buffer management
- ✅ Graceful shutdown with buffer flush
- ✅ Deduplication via `ON CONFLICT DO NOTHING`


## Architecture

```bash
┌─────────────┐      MQTT        ┌───────────┐      MQTT        ┌────────────┐
│  Publisher  │ ───────────────> │ Mosquitto │ ───────────────> │  Ingest    │
│ (5000 msg/s)│                  │  Broker   │                  │  Script    │
└─────────────┘                  └───────────┘                  └─────┬──────┘
                                                                       │
                                                                       │ Batched
                                                                       │ INSERTs
                                                                       ▼
                                                                ┌──────────────┐
                                                                │ TimescaleDB  │
                                                                │ (Hypertable) │
                                                                └──────────────┘
```

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Python 3.12+ with `uv` installed

### Setup

1. **Start the infrastructure** (Mosquitto + TimescaleDB):

```bash
docker compose up -d
```

2. **Install Python dependencies**:

```bash
uv sync
```

3. **Verify the database is ready**:

```bash
docker compose logs timescaledb | grep "database system is ready"
```

## Running the Time-window Batching Demo

Run the unified demo that compares per-message vs. batched ingestion approaches:

```bash
uv run src/per-message-insert.py
```

This single script will:
1. Run naive (per-message) ingestion for 15 seconds
2. Run batched ingestion for 15 seconds
3. Display a comparison table showing the performance difference

**Expected output:**

```bash
======================================================================
MQTT-TO-SQL BATCHING DEMO
======================================================================

Configuration:
  Simulated sensors: 5000
  Publish rate:      1 Hz per sensor
  Total rate:        5,000 messages/second
  Test duration:     15 seconds per test
  Flush interval:    5.0 seconds

Preparing database...
✓ Database cleared

Starting Test 1: Naive Ingestion...

======================================================================
TEST 1: NAIVE INGESTION (Per-Message Inserts)
======================================================================

  Subscribed to sensors/#
  T+   5s | Throughput:  129.8 msg/s | Avg latency:  6.86ms
  T+  10s | Throughput:  137.4 msg/s | Avg latency:  6.84ms
  T+  15s | Throughput:  139.2 msg/s | Avg latency:  6.87ms

Preparing for Test 2...
✓ Database cleared

Starting Test 2: Batched Ingestion...

======================================================================
TEST 2: BATCHED INGESTION (Time-Window Strategy)
======================================================================

  Subscribed to sensors/#
  Batch   1 | Throughput: 4105.4 msg/s | Latency: 479.45ms
  Batch   2 | Throughput: 4541.8 msg/s | Latency: 505.51ms
  Batch   3 | Throughput: 4698.0 msg/s | (final flush)

======================================================================
COMPARISON RESULTS
======================================================================

  Sensor rate: 5,000 msg/s  |  Duration: 15s  |  Total fired: 75,000

  Metric                                        Naive                Batched
  ------------------------------------------------------------------------
  Stored in DB @ T=15s                          2,090                 75,000
  Queue backlog                                72,910                      0
  Throughput (msg/s)                            139.3                 4999.7
  Avg write latency (ms)                         6.87                 328.32

======================================================================
KEY INSIGHT
======================================================================

  The naive pipeline stored 2,090 of 75,000 messages (2.8%).
  Its backlog of 72,910 messages grows by ~4,861 msg/s — it will never recover.

  The batched pipeline stored 75,000 of 75,000 messages (100.0%),
  with a transient backlog of only 0 messages (normal pipeline lag).

✓ Demo complete!
```

**Key Takeaway:** The naive pipeline falls behind immediately and never recovers; its backlog grows faster than it can drain. The batched pipeline keeps pace with the sensor fleet, maintaining a near-zero queue backlog throughout.


### Configuration

Edit `.env` to customize:

```bash
# MQTT Configuration
MQTT_BROKER_HOST=localhost
MQTT_BROKER_PORT=1883

# TimescaleDB Configuration
TIMESCALE_DSN=postgresql://postgres:postgres@localhost:5432/sensors

# Batching Configuration
FLUSH_INTERVAL_SEC=5.0

# Simulated Sensors Configuration
NUM_DEVICES=5000
PUBLISH_RATE_HZ=1
# Total rate: 5000 sensors × 1 Hz = 5000 messages/second
```

## Edge Case Experiments

Interactive experiments demonstrating critical edge cases and best practices.

### Graceful Shutdown

Demonstrates why signal handlers are essential for preventing data loss.

```bash
uv run src/experiments/graceful-shutdown.py
```

**What it does:**
- Runs two parallel ingestion processes (with and without signal handlers)
- Prompts you to press Ctrl+C after 10 seconds
- Compares data loss between both approaches

**Expected output:**

```bash
======================================================================
GRACEFUL SHUTDOWN EXPERIMENT
======================================================================

This experiment demonstrates why signal handlers are critical.

Setup:
  • Publisher rate: 5000 msg/s
  • Flush interval: 30.0s
  • Test runs for 10s, then you interrupt
  • Two parallel ingest processes:
    - WITHOUT graceful shutdown (loses buffer)
    - WITH graceful shutdown (flushes buffer)

✓ Test tables created

Starting ingestion processes...
Starting publisher...

  Publisher connected to MQTT broker

======================================================================
👉 PRESS Ctrl+C NOW TO INTERRUPT BOTH PROCESSES
======================================================================

^C

Interrupting processes...
  [WITH] Caught signal! Flushing buffer...

  Terminating WITHOUT graceful shutdown (hard kill)...
Process Process-1:
  Terminating WITH graceful shutdown (signal handler)...

  [WITH] Caught signal! Flushing buffer...
  [WITH] Flushed 50000 buffered messages
  Waiting for final flush to complete...
    Complete!

======================================================================
COMPARISON RESULTS
======================================================================

Test Duration: 10 seconds
Publisher sent: 50,000 messages (actual)

Approach                            Saved           Lost            Success Rate
----------------------------------------------------------------------
WITHOUT graceful shutdown           0               50,000          0.0           %
WITH graceful shutdown              50,000          0               100.0         %

======================================================================
KEY INSIGHT
======================================================================

✅ Signal handler prevented loss of ~50,000 buffered messages!

✓ Experiment complete!
```

### Duplicate Handling

Demonstrates how unique constraints + `ON CONFLICT DO NOTHING` prevent duplicate data.

```bash
uv run src/experiments/duplicate-handling.py
```

**What it does:**
- Sends 1,000 unique messages via MQTT
- Resends 500 of them (simulating MQTT QoS 1 retries)
- Compares two tables: one WITHOUT unique constraint, one WITH

**Expected output:**

```bash
======================================================================
DUPLICATE HANDLING EXPERIMENT
======================================================================

This experiment demonstrates idempotent inserts using unique constraints.

Setup:
  • Unique messages: 1,000
  • Duplicate messages: 500 (resent)
  • Total messages sent: 1,500
  • Two tables:
    - WITHOUT unique constraint (stores duplicates)
    - WITH unique constraint + ON CONFLICT (deduplicates)

✓ Test tables created

Generating 1,000 unique messages...
Selecting 500 messages to resend (duplicates)...

======================================================================
TEST 1: WITHOUT DEDUPLICATION
======================================================================

Starting ingestion process (no unique constraint)...
Publishing 1,000 unique messages...
Republishing 500 messages (duplicates)...
✓ Test 1 complete

======================================================================
TEST 2: WITH DEDUPLICATION
======================================================================

Starting ingestion process (unique constraint + ON CONFLICT)...
Publishing 1,000 unique messages...
Republishing 500 messages (duplicates)...
✓ Test 2 complete


======================================================================
COMPARISON RESULTS
======================================================================

Messages sent: 1,500
  • Unique: 1,000
  • Duplicates: 500

Approach                            Rows Stored     Duplicates
----------------------------------------------------------------------
WITHOUT deduplication               1,500           500
WITH deduplication                  1,000           0

Sample duplicates in 'test_without_dedup':
  • 2026-02-18 14:57:54+00:00 | sensor-000 | 2 copies
  • 2026-02-18 14:57:54.000001+00:00 | sensor-001 | 2 copies
  • 2026-02-18 14:57:54.000002+00:00 | sensor-002 | 2 copies

======================================================================
KEY INSIGHT
======================================================================

✅ Unique constraint + ON CONFLICT prevented 500 duplicate inserts!

This pattern ensures idempotent ingestion:
  • MQTT QoS 1 can redeliver messages
  • Network retries won't create duplicates
  • Multiple publishers can send same data safely

✓ Experiment complete!
```

**Configuration:** Adjust in `.env`:

```bash
EXPERIMENT_UNIQUE_MESSAGES=1000      # Number of unique messages
EXPERIMENT_DUPLICATE_MESSAGES=500    # Number to resend (duplicates)
```

### Error Recovery (Poison Pill)

Demonstrates how to handle data quality issues without losing entire batches.

```bash
uv run src/experiments/error-recovery.py
```

**What it does:**
- Sends 200 valid messages and 1 "poison pill" (invalid data type)
- Compares **Naive** ingestion (batch fails, loses all data) vs **Robust** ingestion (catches error, retries row-by-row)

**Expected output:**

```bash
======================================================================
ERROR RECOVERY EXPERIMENT (POISON PILL)
======================================================================

This experiment demonstrates handling of 'poison pill' messages (invalid data).
Setup:
  • Batch size: 201 messages
  • Contains: 200 valid + 1 poison pill
  • Poison pill: String value in numeric column ('check-engine-light')

✓ Test tables created

======================================================================
TEST 1: NAIVE INGESTION (Fails Batch)
======================================================================
  Sending 100 valid messages...
  Sending 1 POISON PILL message (invalid type)...
  Sending 100 more valid messages...
  [NAIVE] 💥 Batch failed! Error: invalid input syntax for type double precision: "check-engine-light"
CONTEXT:  unnamed portal parameter $4 = '...'
  [NAIVE] ❌ Dropping batch of 201 messages
✓ Test 1 complete

======================================================================
TEST 2: ROBUST INGESTION (Row-by-Row Fallback)
======================================================================
  Sending 100 valid messages...
  Sending 1 POISON PILL message (invalid type)...
  Sending 100 more valid messages...
  [ROBUST] ⚠️ Batch failed. Falling back to row-by-row...
  [ROBUST] 🚫 Sidelined bad message: check-engine-light
  [ROBUST] Recovered 200 valid messages. Lost 1 bad messages.
✓ Test 2 complete


======================================================================
COMPARISON RESULTS
======================================================================

Valid messages sent: 200
Poison pills sent: 1

Approach                            Saved           Lost            Success Rate
----------------------------------------------------------------------
NAIVE (Batch fail)                  0               200             0.0%
ROBUST (Row recovery)               200             0               100.0%

======================================================================
KEY INSIGHT
======================================================================

✅ Robust ingestion saved 200 messages by isolating the error!
❌ Naive ingestion dropped the entire batch because of 1 bad record.

✓ Experiment complete!
```

**Configuration:** Adjust in `.env`:

```bash
EXPERIMENT_VALID_MESSAGES_BATCH=100  # Batch size
EXPERIMENT_POISON_PILL_MESSAGE='...' # Invalid JSON message
```

### Back-Pressure / Load Testing

Demonstrates system behavior under increasing load.

```bash
uv run src/experiments/back-pressure-overload.py
```

**What it does:**
- Runs 3 phases with increasing load (1k, 5k, 30k msg/s).
- Tracks critical metrics:
  - **Latency:** Time to insert a batch (spike > flush interval = danger).
  - **Queue Depth:** Estimated number of messages waiting in buffer/broker.
  - **Efficiency:** % of sent messages successfully processed.

**Key Insight:** When "Queue Depth" grows and Latency >> Flush Interval, your system is falling behind. Phase 2 mirrors the main demo's load (5k msg/s): single consumer handles it fine. Phase 3 (30k msg/s) is where it breaks, which motivates the scaling experiment.

**Configuration:** adjust `EXPERIMENT_TIMEOUT_SEC` in `.env` if your system is slower.

**Expected output:**

```bash
======================================================================
BACK-PRESSURE / LOAD TEST EXPERIMENT
======================================================================

Simulating increasing load to demonstrate system limits.


======================================================================
BATCH INGESTION TEST 1: 1000 DEVICES @ 1HZ (~1000 msg/s)
======================================================================

  Subscribed to sensors/backpressure/#
  Batch   1:  4200 rows | Throughput:  818.3 msg/s | Latency: 127.08ms
  Batch   2:  5100 rows | Throughput:  996.8 msg/s | Latency: 103.16ms
  Batch   3:  5100 rows | Throughput:  996.4 msg/s | Latency: 104.39ms
  Batch   4:  5100 rows | Throughput:  997.1 msg/s | Latency: 101.47ms
  Batch   5:  5200 rows | Throughput: 1012.9 msg/s | Latency: 119.50ms
  Waiting for publisher stats...
  [Publisher] Stopping...
  Waiting for publisher to exit...
----------------------------------------------------------------------
  Summary:
  - Messages sent:      24,700
  - Messages processed: 24,700
  - Efficiency:         100.0%
  - Queue Depth:        0
======================================================================


======================================================================
BATCH INGESTION TEST 2: 5000 DEVICES @ 1HZ (~5000 msg/s)
======================================================================

  Subscribed to sensors/backpressure/#
  Batch   1: 22059 rows | Throughput: 4090.4 msg/s | Latency: 387.76ms
  Batch   2: 27439 rows | Throughput: 5005.2 msg/s | Latency: 472.18ms
  Batch   3: 27300 rows | Throughput: 4985.6 msg/s | Latency: 463.01ms
  Batch   4: 27500 rows | Throughput: 5007.7 msg/s | Latency: 480.71ms
  Batch   5: 27500 rows | Throughput: 5001.9 msg/s | Latency: 485.27ms
  Waiting for publisher stats...
  [Publisher] Stopping...
  Waiting for publisher to exit...
----------------------------------------------------------------------
  Summary:
  - Messages sent:      131,800
  - Messages processed: 131,798
  - Efficiency:         100.0%
  - Queue Depth:        2
======================================================================


======================================================================
BATCH INGESTION TEST 3: 30000 DEVICES @ 1HZ (~30000 msg/s)
======================================================================

  Subscribed to sensors/backpressure/#
  Batch   1: 106105 rows | Throughput: 16309.7 msg/s | Latency: 1499.30ms
  Batch   2: 138780 rows | Throughput: 19834.0 msg/s | Latency: 1965.39ms
  Batch   3: 139151 rows | Throughput: 19735.7 msg/s | Latency: 1998.25ms
  Batch   4: 136743 rows | Throughput: 19514.3 msg/s | Latency: 1959.45ms
  Batch   5: 139032 rows | Throughput: 19875.5 msg/s | Latency: 1956.56ms
  Waiting for publisher stats...
  [Publisher] Stopping...
  Waiting for publisher to exit...
----------------------------------------------------------------------
  Summary:
  - Messages sent:      1,008,000
  - Messages processed: 665,905
  - Efficiency:         66.1%
  - Queue Depth:        342,095
======================================================================


Experiment complete.
```

**Configuration:** Adjust in `.env`:

```bash
EXPERIMENT_PHASE_1_DEVICES=1000   # Low load (~1k msg/s)
EXPERIMENT_PHASE_2_DEVICES=5000   # Medium load (~5k msg/s, mirrors main demo)
EXPERIMENT_PHASE_3_DEVICES=30000  # High load — breaking point (~30k msg/s)
EXPERIMENT_RATE_HZ=1              # Messages per second per device
EXPERIMENT_TIMEOUT_SEC=60         # Timeout for batch inserts
```

### Back-pressure Scaling Test

Demonstrates how to handle high load by horizontally scaling consumer processes.

```bash
uv run src/experiments/back-pressure-scaling.py
```

**What it does:**

- Runs the same 30k msg/s load that broke the single consumer in Phase 3 above.
- Spawns 3 consumer + 3 publisher processes using **topic-based partitioning**.
- Each publisher publishes to its own partition topic (`sensors/backpressure/p{N}/data`).
- Each consumer subscribes to its own partition, getting the full MQTT throughput per connection.
- Writes to TimescaleDB concurrently using the COPY protocol.

**Key Insight:** Topic-based partitioning gives each consumer its own MQTT throughput channel (~16k msg/s with QoS 1), achieving near-linear scaling. Shared subscriptions would *split* that throughput, making 3 consumers worse than 1.

**Expected Output:**

```bash
======================================================================
BACK-PRESSURE / HORIZONTAL SCALING EXPERIMENT
======================================================================


======================================================================
SCALING TEST: 30000 DEVICES @ 1HZ (~30000 msg/s)
Spawning 3 Consumers
======================================================================

[Consumer-3] Starting loop...
[Consumer-2] Starting loop...
[Consumer-1] Starting loop...
  [Consumer-3] Batch   1: 51577 rows | Latency: 1152.13ms
  [Consumer-1] Batch   1: 63142 rows | Latency: 2306.75ms
  [Consumer-2] Batch   1: 63070 rows | Latency: 2313.87ms
  [Consumer-3] Batch   2: 63935 rows | Latency: 1389.98ms
  [Consumer-1] Batch   2: 65603 rows | Latency: 1553.29ms
  [Consumer-2] Batch   2: 65604 rows | Latency: 1554.75ms
  [Consumer-3] Batch   3: 63587 rows | Latency: 1319.62ms
  [Consumer-1] Batch   3: 65533 rows | Latency: 1513.99ms
  [Consumer-2] Batch   3: 66029 rows | Latency: 1534.75ms
  [Consumer-3] Batch   4: 63271 rows | Latency: 1300.36ms
  [Consumer-1] Batch   4: 65341 rows | Latency: 1512.40ms
  [Consumer-2] Batch   4: 65694 rows | Latency: 1549.76ms
  [Consumer-3] Batch   5: 63396 rows | Latency: 1315.39ms
  [Consumer-1] Batch   5: 65659 rows | Latency: 1532.37ms
  [Consumer-2] Batch   5: 65814 rows | Latency: 1548.72ms
  Waiting for publisher stats...
  [Publisher-2] Stopping...
  [Publisher-1] Stopping...
  [Publisher-3] Stopping...
----------------------------------------------------------------------
  Summary:
  - Messages sent:      979,500
  - Messages processed: 979,499
  - Efficiency:         100.0%
  - Queue Depth:        1
======================================================================


Experiment complete.
```

**Configuration:** Adjust in `.env`:

```bash
EXPERIMENT_PHASE_3_DEVICES=30000  # Target load (shared with overload experiment)
EXPERIMENT_CONSUMER_COUNT=3       # Number of consumer + publisher pairs
```

---

## Exploring Hypertable Internals

Use the provided SQL queries to explore TimescaleDB features:

```bash
cat db/queries.sql | docker compose exec -T timescaledb psql -U postgres -d sensors
```

Or copy individual queries from `db/queries.sql` to explore:
- Hypertable configuration
- Chunk information and sizes
- Index details
- Data distribution
- Compression settings

**Expected output:**

```bash
cat db/queries.sql | docker compose exec -T timescaledb psql -U postgres -d sensors
 hypertable_name | num_dimensions | num_chunks | compression_enabled | tablespaces
-----------------+----------------+------------+---------------------+-------------
 sensor_readings |              1 |          1 | f                   |
(1 row)

    chunk_name     |      range_start       |       range_end        | is_compressed | chunk_tablespace
-------------------+------------------------+------------------------+---------------+------------------
 _hyper_1_97_chunk | 2026-02-13 00:00:00+00 | 2026-02-14 00:00:00+00 | f             |
(1 row)

                  chunk                  | total_size | index_size | toast_size | table_size
-----------------------------------------+------------+------------+------------+------------
 _timescaledb_internal._hyper_1_97_chunk | 40 kB      | 32 kB      |            | 8192 bytes
(1 row)

       indexname        |                                                 indexdef
------------------------+-----------------------------------------------------------------------------------------------------------
 idx_readings_dedup     | CREATE UNIQUE INDEX idx_readings_dedup ON public.sensor_readings USING btree (ts, device_id, metric_type)
 sensor_readings_ts_idx | CREATE INDEX sensor_readings_ts_idx ON public.sensor_readings USING btree (ts DESC)
(2 rows)

 column_name | segmentby_column_index | orderby_column_index | orderby_asc | orderby_nullsfirst
-------------+------------------------+----------------------+-------------+--------------------
(0 rows)

   device_id    | reading_count |         first_reading         |         last_reading
----------------+---------------+-------------------------------+-------------------------------
 sensor-test    |             3 | 2026-02-13 18:22:07.594191+00 | 2026-02-13 18:22:07.598695+00
 sensor-valid-1 |             1 | 2026-02-13 18:22:10.731698+00 | 2026-02-13 18:22:10.731698+00
 sensor-valid-2 |             1 | 2026-02-13 18:22:10.731744+00 | 2026-02-13 18:22:10.731744+00
(3 rows)

 metric_type | reading_count | avg_value | min_value | max_value
-------------+---------------+-----------+-----------+-----------
 humidity    |             1 |     65.00 |     65.00 |     65.00
 pressure    |             1 |   1013.20 |   1013.20 |   1013.20
 temperature |             3 |     25.67 |     25.50 |     26.00
(3 rows)

 device_id | metric_type | ts | prev_ts | gap
-----------+-------------+----+---------+-----
(0 rows)

 total_rows | total_size
------------+------------
          5 | 16 kB
(1 row)

 hypertable_name | column_name | time_interval
-----------------+-------------+---------------
 sensor_readings | ts          | 1 day
(1 row)
```

## Production Enhancements

This is a tutorial demo. For production, consider:

1. **Monitoring**: Instrument flush latency, queue depth, and throughput — these metrics tell you when to scale or tune the flush interval before problems compound.
2. **Compression**: Enable Hypercore compression for chunks older than your active query window (start with 7 days). See `db/schema.sql` for the compression settings.
3. **Query optimization**: Use `EXPLAIN ANALYZE` on your read patterns and add composite indexes accordingly. Continuous aggregates can pre-compute hourly/daily summaries for dashboards.
4. **Scaling**: When a single consumer can't keep up, layer on topic-based partitioning as shown in the back-pressure experiments. Each additional consumer+publisher pair adds ~16k msg/s capacity.
5. **Back-pressure strategies**: Alternatives to horizontal scaling include adaptive flush intervals (shorten the interval as queue depth grows), flow control (pause the MQTT subscriber when the buffer exceeds a threshold), and PostgreSQL tuning (`shared_buffers`, `max_wal_size`, `checkpoint_timeout`).
6. **Connection pooling**: Use `psycopg_pool.ConnectionPool` when running multiple consumer processes to avoid exhausting database connections.
7. **Error recovery**: Add a dead-letter queue for poison-pill messages rather than discarding them — the row-by-row fallback in `error-recovery.py` shows the isolation pattern.

## Cleanup

Stop and remove containers:

```bash
docker compose down -v
```
