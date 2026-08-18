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
| M1 | Local infrastructure | Both brokers and the warehouse boot reproducibly | 2 d | M0 |
| M2 | Harness core and Kafka driver | A valid, self-measured benchmark run | 3–4 d | M1 |
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

## M1 — Local infrastructure

**Goal.** Anyone can bring up either broker plus the Iceberg warehouse with one
command, reproducibly, within a 16 GB memory budget.

### Tasks

- [ ] Lock the version matrix: verify real, currently-published image tags for
      Kafka, Pulsar, Flink, Spark, Iceberg REST, MinIO, and Airflow. Resolve each
      to a digest. Record in `infra/compose/versions.env`.
- [ ] `docker-compose.core.yml` — MinIO plus bucket bootstrap, Iceberg REST catalog
- [ ] `docker-compose.kafka.yml` — single-broker KRaft, no ZooKeeper
- [ ] `docker-compose.pulsar.yml` — standalone to begin with (see Q-1)
- [ ] Compose profiles so `core+kafka` and `core+pulsar` are independently bootable
- [ ] Explicit `cpus` and `mem_limit` on every service, values recorded in `infra/conf/`
- [ ] Healthchecks on every service; `make up` blocks until genuinely ready, not
      merely until the container is running
- [ ] `make up` / `make down` / `make nuke` (the last removing volumes, per invariant 4)
- [ ] `scripts/preflight.sh` — check Docker memory allocation, free disk, port conflicts
- [ ] ADR-0001 recording the version matrix and why each version was chosen
- [ ] Document Docker Hub rate limits and the recommended `docker login` before
      first bulk pull

### Exit criteria

- `make up PROFILE=kafka` then `make down` is clean and repeatable
- Same for `pulsar`
- Peak resident memory for either profile is recorded and under 8 GB
- Cold start from a clean Docker state is timed and documented (NFR-3)

### Watch for

Pulsar standalone is the heaviest single container here. If it does not fit the
budget alongside the warehouse, that forces Q-1 earlier than planned.

---

## M2 — Harness core and Kafka driver

**Goal.** A single command produces one valid, trustworthy benchmark run against
Kafka. This milestone is the heart of the project.

### Tasks

**Configuration and structure**
- [ ] `harness/pyproject.toml`, Python 3.12, ruff + mypy strict + pytest
- [ ] Config schema (pydantic): message size, target rate, duration, warm-up,
      partitions, durability, batching, compression, key cardinality, producer
      and consumer counts
- [ ] `configs/smoke.yaml` — a small, fast run used for development

**Measurement core (invariants 1 and 2 live here)**
- [ ] Open-loop rate scheduler: emits intended-send timestamps from the target
      rate, independent of send completion. **Unit-tested against a deliberately
      stalled sink** to prove that a stall widens the measured distribution
      rather than vanishing from it.
- [ ] Payload generator with embedded sequence number and monotonic send timestamp
- [ ] HdrHistogram capture, plus merge across producer and consumer threads
- [ ] Consumer-side validation: gap, duplicate, and reordering detection (FR-4)
- [ ] Warm-up sample discard, with the discarded window recorded (M-5)
- [ ] Validity gate: mark the run invalid if achieved rate falls short of target
      by more than a configured tolerance (M-8)

**Driver abstraction**
- [ ] `drivers/base.py` — the interface both transports implement. Keep it narrow;
      everything not transport-specific stays in shared code (invariant 3).
- [ ] `drivers/kafka_driver.py` using `confluent-kafka`
- [ ] Topic lifecycle: create with the configured partition count before the run,
      delete after

**Self-measurement (NFR-5)**
- [ ] A null/loopback driver that skips the broker entirely, establishing the
      harness's own latency floor and maximum sustainable rate
- [ ] Document that floor. Any benchmark result within a small multiple of it is
      measuring the harness, not the broker, and must be reported as such.

**Output**
- [ ] Environment fingerprint: CPU model and count, RAM, kernel, Docker version,
      resolved image digests (M-3)
- [ ] Run manifest written to `results/<run_id>/` as JSON plus raw histogram
- [ ] `make bench CONFIG=...`

### Exit criteria

- A smoke run against Kafka completes and emits a manifest with a valid flag
- The loopback floor is measured and documented in the README
- Rate-scheduler and histogram-merge unit tests pass
- Ten repeat runs of `smoke.yaml` agree within a variance band, and that band is
  written down (NFR-6)

### Watch for

If the loopback floor turns out to be close to expected broker latencies, stop
and reconsider the harness language for the measurement path before building
anything on top of it. Discovering this at M8 would be expensive.

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
- [ ] ADR-0002 recording the equivalence decisions
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
- [ ] ADR-0003 on the SQL-versus-Java decision

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
| ADR-0001 | Version matrix | Pending M1 |
| ADR-0002 | Kafka/Pulsar config equivalence | Pending M3 |
| ADR-0003 | Flink job implementation | Pending M5 |
