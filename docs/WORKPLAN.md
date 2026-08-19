# Workplan — kafka-pulsar-bench

**Version:** 0.1
**Date:** 2026-08-18
**Traces to:** `docs/BUSINESS_REQUIREMENTS.md`

---

## Sequencing principle

The harness is a measuring instrument. An instrument that is convenient but
wrong is worse than no instrument, because it produces confident bad numbers.

So the order is: **build the instrument, prove it is honest, then use it.**
Milestones 2–4 build and validate measurement. Only at Milestone 8 does the
project make any claim about which broker is faster. Flink, Spark, and Airflow
(M5–M7) are consumers of a results warehouse that already works; they are
deliberately sequenced after the measurement core rather than interleaved, so a
broken DAG can never be confused with a broken measurement.

Two consequences worth stating up front:

- **M3 is the real risk milestone.** Config equivalence between Kafka and Pulsar
  is where this project either earns credibility or quietly becomes another
  unfair benchmark. Budget generously and expect to redo parts of it.
- **M2 must include a self-measurement step.** If the harness saturates before
  either broker does, every subsequent milestone is measuring Python.

## Milestone overview

| # | Milestone | Outcome | Rough effort | Depends on |
|---|---|---|---|---|
| M0 | Scaffolding and requirements | Repo, docs, conventions | 0.5 d — **done** | — |
| M1 | Local infrastructure | Both brokers and the warehouse boot reproducibly | 2 d — **done** | M0 |
| M2 | Harness core and Kafka driver | A valid, self-measured benchmark run | 3–4 d — **done** | M1 |
| M3 | Pulsar driver and fairness contract | A defensible like-for-like comparison | 3–5 d | M2 |
| M4 | Results warehouse | Every run durable and queryable in Iceberg | 2 d | M2 |
| M5 | Flink pipeline | Both brokers to Iceberg, pipeline metrics captured | 3 d | M1, M4 |
| M6 | PySpark analysis | Percentiles, comparisons, regression detection | 2 d | M4 |
| M7 | Airflow orchestration | Unattended sweeps and Iceberg maintenance | 2 d | M4, M6 |
| M8 | Methodology and baseline results | Published, reviewable, reproducible results | 3 d | all |

Effort is focused working days, not calendar days. M4 can run in parallel with
M3 if desired; nothing else on the critical path can.

---

## M0 — Scaffolding and requirements ✅

**Goal.** Repository exists with conventions and requirements agreed before code.

- [x] Directory tree, `.gitignore`, `.gitattributes` (LF enforcement), MIT licence
- [x] Public GitHub remote, `thompgt/kafka-pulsar-bench`
- [x] `docs/BUSINESS_REQUIREMENTS.md`
- [x] `CLAUDE.md` conventions and invariants
- [x] `docs/WORKPLAN.md`

**Exit criteria.** Docs pushed; scope and methodology requirements agreed.

---

## M1 — Local infrastructure ✅

**Goal.** Anyone can bring up either broker plus the Iceberg warehouse with one
command, reproducibly, within a 16 GB memory budget.

### Tasks

- [x] Lock the version matrix against the registries — ADR-0001
- [x] `docker-compose.core.yml` — MinIO plus bucket bootstrap, Iceberg REST catalog
- [x] `docker-compose.kafka.yml` — single-broker KRaft, no ZooKeeper
- [x] `docker-compose.pulsar.yml` — standalone (Q-1 still open)
- [x] Independently bootable `core+kafka` and `core+pulsar`
- [x] Explicit `cpus` and `mem_limit` on every service
- [x] Healthchecks that test a real API call, not process liveness
- [x] `up` / `down` / `nuke` / `reset-broker`
- [x] `scripts/preflight.sh` — memory, disk, ports, foreign containers
- [x] ADR-0001 recording the version matrix
- [x] Docker Hub rate limits documented

### Outcome

Both profiles verified working: Kafka topic create/produce/consume round-trip,
Pulsar partitioned-topic round-trip, and the Iceberg REST catalog answering
`/v1/config`. Broker tuning env vars confirmed applied to real topic configs.

| Profile | Resident memory | Cold start incl. pull |
|---|---|---|
| core + Kafka | ~0.78 GB | ~1m |
| core + Pulsar | ~1.75 GB | ~3m20s |

Both inside the 8 GB exit criterion, leaving headroom for an engine alongside.

### Found while verifying

- Git Bash rewrites absolute container paths on `docker exec`; needs
  `MSYS_NO_PATHCONV=1`. Recorded in `CLAUDE.md`.
- `docker ps` renders contiguous port mappings as a range, which made the
  preflight port check report MinIO's own ports as foreign conflicts.
- 13 unrelated containers were running on the dev host. They contend for CPU
  and page cache, so preflight now warns, and `STRICT=1` makes it an error.

## M2 — Harness core and Kafka driver ✅

**Goal.** A single command produces one valid, trustworthy benchmark run against
Kafka. This milestone is the heart of the project.

### Delivered

- [x] `harness/` package, Python 3.12, ruff + mypy config, pytest
- [x] Pydantic run config; `configs/smoke.yaml`, `configs/loopback-floor.yaml`
- [x] Open-loop rate scheduler, unit-tested against a deliberately stalled sink
- [x] Payload codec with embedded sequence and timing header
- [x] HdrHistogram capture and merge; sample capture kept out of the hot path
- [x] Gap, duplicate and reordering detection
- [x] Warm-up discard, recorded in the manifest
- [x] Validity gate: rate starvation, loss, duplication, buffer overflow, platform
- [x] `drivers/base.py`, `kafka_driver.py`, `loopback.py`
- [x] Loopback floor measured and documented — ADR-0002
- [x] Environment fingerprint with live container digests
- [x] Run manifest, `kpbench run` / `kpbench show`
- [x] `--warmup-runs` implementing the ADR-0004 procedure
- [x] 32 unit tests passing

### Exit criteria — met

- Smoke run against Kafka completes with a valid manifest ✅
- Loopback floor measured and documented ✅ (ADR-0002)
- Rate-scheduler and histogram-merge tests pass ✅
- Ten repeat runs agree within a documented band ✅ (ADR-0004)

### Four defects found, all of which produced plausible numbers rather than errors

This is the milestone's real output. Each of these would have survived into
published results if the harness had not been measured against itself first.

| # | Defect | Symptom | Found by |
|---|---|---|---|
| 1 | Spin window exceeded the send interval, so the producer busy-spun and held the GIL | 9.4 ms p50 on an in-process loopback | loopback floor |
| 2 | Three pure-Python histogram writes per message in the consumer | 255 ms p99 at 20k/s, from the harness's own backlog | loopback rate sweep |
| 3 | `consume()` blocks its full timeout unless the batch fills, pinning consumer capacity at the production rate | 2.65 s p50 against Kafka | comparison against a 5.5 ms raw RTT |
| 4 | Windows quantises poll waits to the ~15.6 ms scheduler tick | 83 ms p50 vs 26 ms for identical code under WSL2 | isolating `consume()` on an idle topic |

### Outcome

| | Linux (WSL2) | Windows host |
|---|---|---|
| Supported rate | ≤ 60k/s | rejected |
| Usable rate | ≤ 100k/s | — |
| Floor p50 | ~200 us | ~450 us |
| Floor p99 | 1.3–6 ms | 2.6–222 ms |

Kafka smoke at 2,000/s: p50 ~15 ms, p99 ~65 ms on a warm broker, on a contended
host. Variance band 1.3x on p99, 1.7x on p50.

### Carried forward

- Re-measure floor and band on an idle host (`STRICT=1`) before publication
- The p99 floor is still the same order as Kafka's p99; absolute tail figures
  are not comparable with vendor numbers, and every result must say so
- Separate producer/consumer processes now look unnecessary at a 100k/s ceiling

---

## M3 — Pulsar driver and fairness contract

**Goal.** The comparison is defensible. This is the milestone most likely to
consume more time than planned, and the one most worth doing slowly.

### Tasks

- [ ] `drivers/pulsar_driver.py` using the official `pulsar-client`
- [ ] Topic/namespace lifecycle with a partition count matching the Kafka side
- [ ] **The equivalence table.** For every workload parameter, document the Kafka
      setting, the Pulsar setting, whether they are truly equivalent, and the
      justification. At minimum:
      - durability: `acks` and `min.insync.replicas` versus ensemble/write/ack quorum
      - fsync behaviour: Kafka page-cache-and-flush policy versus BookKeeper journal
      - partitioning: Kafka partitions versus Pulsar partitioned topics
      - batching: `linger.ms` and `batch.size` versus Pulsar batching delay and max
      - compression: codec availability and defaults on each side
      - consumer model: consumer group versus subscription type
- [ ] Flag every parameter where exact equivalence is **impossible**, and state
      which way the residual bias runs
- [ ] Where equivalence is genuinely ambiguous, run both settings rather than
      picking one and defending it
- [ ] Loopback floor re-measured through the Pulsar client, to confirm the two
      client libraries do not impose materially different harness overheads
- [ ] Symmetry audit: diff the two drivers and confirm nothing fairness-relevant
      exists in one and not the other
- [ ] ADR-0005 recording the equivalence decisions
- [ ] Resolve Q-1 (standalone versus multi-bookie) and record the reasoning

### Exit criteria

- The same config file runs against both brokers and produces comparable manifests
- The equivalence table is complete, with no parameter left undocumented
- Client overhead difference between the two harness paths is measured and stated

### Watch for

The temptation to declare equivalence where there is none, because the table
looks cleaner. An honest "these are not equivalent, and the bias favours X" is
worth more than a tidy table.

---

## M4 — Results warehouse

**Goal.** Every run is durable, queryable, and provenance-complete.

### Tasks

- [ ] Iceberg schema design:
      - `bench.runs` — run_id, timestamp, broker, config (nested), environment
        fingerprint (nested), validity, summary statistics, supersedes reference
      - `bench.latency_samples` — run_id plus histogram buckets
      - `bench.throughput_series` — run_id, time bucket, achieved rate
- [ ] Partitioning strategy: by broker and run date. Document why.
- [ ] Table creation via `pyiceberg` against the REST catalog
- [ ] Loader: `results/<run_id>/` to Iceberg, idempotent on run_id
- [ ] Append-only enforcement, with the supersedes path tested (invariant 5, FR-9)
- [ ] Schema evolution test: add a config field, confirm historical runs still read
- [ ] `make load RUN=<run_id>`, and an auto-load option at the end of `make bench`

### Exit criteria

- Runs from both brokers are queryable from the catalog
- Re-loading the same run is a no-op rather than a duplicate
- A new config field can be added without rewriting history

---

## M5 — Flink pipeline

**Goal.** A realistic streaming consumer, demonstrating the lakehouse path and
capturing pipeline-level behaviour.

Note the deliberate separation: **Flink is not the latency measurement
instrument.** Per-message latency comes from the harness (M2). Flink is measured
for pipeline characteristics, which are a different question.

### Tasks

- [ ] Resolve Q-2: Flink SQL versus a Java DataStream job. Prefer SQL if it
      suffices; it removes an entire build toolchain. If Java is needed, build the
      jar inside a container — no local Maven is assumed.
- [ ] `docker-compose.flink.yml` with JobManager and TaskManager, checkpointing
      configured to MinIO
- [ ] Source connectors for both Kafka and Pulsar, with identical downstream logic
- [ ] Iceberg sink writing Parquet into a `bench.stream_events` table
- [ ] Capture checkpoint duration and size, backpressure, sustained sink
      throughput, and restart count into `bench.pipeline_metrics` (FR-12)
- [ ] Document the small-files behaviour observed, which motivates M7 maintenance
- [ ] ADR-0006 on the SQL-versus-Java decision

### Exit criteria

- The same logical job runs against both brokers and writes to Iceberg
- Pipeline metrics are captured for a sustained run
- Checkpointing recovers correctly after a forced TaskManager kill

---

## M6 — PySpark analysis

**Goal.** Turn stored runs into defensible comparisons.

### Tasks

- [ ] Spark session configured against the Iceberg REST catalog and MinIO
- [ ] Percentile computation from **merged** histograms (invariant 2), never from
      averaged percentiles
- [ ] Per-sweep comparison: latency percentiles and throughput curves, Kafka
      versus Pulsar, with invalid runs excluded and the exclusions reported
- [ ] Variance analysis across repeat runs, feeding the NFR-6 band
- [ ] Regression detection against a named baseline sweep (FR-14)
- [ ] Chart generation into `docs/results/`, readable in both light and dark
- [ ] Every generated table and chart is annotated with the config file and
      environment fingerprint that produced it (M-9)

### Exit criteria

- One command regenerates every published chart from the warehouse
- Invalid and excluded runs are visible in the output, not silently dropped

---

## M7 — Airflow orchestration

**Goal.** Sweeps run unattended and correctly, including the teardown discipline
that makes them valid.

### Tasks

- [ ] `docker-compose.airflow.yml` — api-server, scheduler, triggerer, Postgres,
      with `AIRFLOW_UID` handled for WSL2
- [ ] `sweep_dag`: iterate the parameter matrix; for each point, rebuild the
      broker from clean, run the benchmark, load results, tear down (invariant 4)
- [ ] Sweep definitions as YAML under `configs/sweeps/`
- [ ] Partial-result durability: an aborted sweep keeps completed runs (FR-16)
- [ ] Retry policy that does **not** silently retry into a contaminated
      environment — a retry must rebuild the broker first
- [ ] `maintenance_dag`: Iceberg compaction, snapshot expiry, orphan-file cleanup
- [ ] Keep Airflow as an orchestrator only. It triggers and monitors; it does not
      move data.

### Exit criteria

- A multi-point sweep across both brokers runs unattended to completion
- Killing the sweep midway leaves completed runs intact and queryable
- Maintenance measurably reduces file count on a fragmented table

---

## M8 — Methodology and baseline results

**Goal.** Publish something a sceptic can attack, and survive the attack.

### Tasks

- [ ] `docs/METHODOLOGY.md` addressing every M-1 through M-9 requirement explicitly,
      including the equivalence table from M3 and the harness floor from M2
- [ ] Resolve Q-3 and run the canonical baseline sweep
- [ ] Publish results into `docs/results/` with charts, environment, and the
      config files that produced them
- [ ] A **limitations** section written before anyone else has to point them out:
      single machine, single broker node, client libraries included, untuned
      configurations, no cross-AZ behaviour
- [ ] README rewritten around real results, replacing the current stub
- [ ] Reproduction instructions verified on a clean machine, end to end
- [ ] Retrospective: which of the seven invariants proved hardest to hold, and why

### Exit criteria

All six success criteria in `docs/BUSINESS_REQUIREMENTS.md` section 8 are met.

---

## Deferred

Explicitly not now, recorded so they stop competing for attention:

- A third broker (Redpanda is the obvious candidate)
- Multi-node or cloud deployment
- Managed-service comparison
- Continuous benchmarking in CI
- A public results dashboard

## Decision log

| ID | Decision | Status |
|---|---|---|
| Q-1 | Pulsar standalone versus multi-bookie | Open — due M3 |
| Q-2 | Flink SQL versus Java DataStream | Open — due M5 |
| Q-3 | Canonical baseline sweep | Open — due M8 |
| ADR-0001 | Version matrix | Accepted |
| ADR-0002 | Harness operating envelope | Accepted (revised) |
| ADR-0003 | Benchmarks run under Linux | Accepted |
| ADR-0004 | Variance band and broker warm-up | Accepted |
| ADR-0005 | Kafka/Pulsar config equivalence | Pending M3 |
| ADR-0006 | Flink job implementation | Pending M5 |
