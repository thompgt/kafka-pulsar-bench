# Business Requirements — kafka-pulsar-bench

**Version:** 0.1 (draft)
**Date:** 2026-08-18
**Owner:** Thomas Pequegnot
**Status:** Approved for build

---

## 1. Purpose

Teams choosing between Apache Kafka and Apache Pulsar are usually forced to rely
on vendor-published benchmarks, each produced by a party with an interest in the
outcome, on hardware nobody else has, using a methodology rarely described in
enough detail to reproduce.

This project builds an **independent, open, reproducible harness** that measures
both systems under identical workloads and publishes both the numbers and the
complete means of regenerating them.

The deliverable is the *harness and its methodology*. Benchmark numbers are an
output of the deliverable, not the deliverable itself.

## 2. Objectives

| # | Objective | Measure of success |
|---|---|---|
| O-1 | Make Kafka and Pulsar comparable under a genuinely identical workload | A documented config-equivalence table, reviewed and defensible |
| O-2 | Make every published number reproducible by a third party | `make bench CONFIG=<file>` reproduces a published run within stated variance |
| O-3 | Make results durable, queryable, and comparable over time | Every run is a row in an Iceberg table with full provenance |
| O-4 | Avoid the standard benchmarking errors that invalidate most published results | Methodology doc explicitly addresses each; see section 5 |
| O-5 | Demonstrate a working streaming-lakehouse pipeline end to end | Flink to Iceberg to PySpark path operational for both brokers |

## 3. Stakeholders

| Stakeholder | Interest |
|---|---|
| Primary author | Deep operational familiarity with all six technologies; a portfolio artefact |
| Engineering teams evaluating brokers | A neutral starting point and a harness they can re-run on their own hardware |
| Reviewers and sceptics | Enough methodological detail to attack the results credibly |

## 4. Scope

### 4.1 In scope

- Apache Kafka (KRaft mode) and Apache Pulsar (standalone, and later multi-bookie)
- Single-machine local deployment via Docker Compose
- Producer to broker to consumer end-to-end latency, and sustained throughput
- Parameter sweeps across message size, target rate, partition count, durability
  setting, batching, and compression
- A Flink consumer writing to Iceberg, measured for pipeline-level behaviour
  (checkpoint duration, backpressure, sustained sink throughput)
- PySpark analysis producing per-run and run-over-run comparisons
- Airflow orchestration of the sweep matrix and Iceberg table maintenance

### 4.2 Out of scope

- Multi-node, cloud, or cross-AZ deployment. Single-machine only; results are
  explicitly **not** a proxy for distributed production performance.
- Managed offerings (Confluent Cloud, StreamNative Cloud, MSK). Self-hosted OSS only.
- Other brokers (Redpanda, NATS, RabbitMQ, Kinesis). The architecture should not
  *preclude* adding one, but none will be added in this phase.
- Declaring a general-purpose winner. Conclusions are scoped to the measured
  configurations on the stated hardware.
- Tuning either broker to its theoretical maximum. Both run at documented,
  defensible configurations, not at expert-tuned extremes.

## 5. Methodology requirements

These are requirements, not implementation detail, because violating any of them
invalidates the entire deliverable.

| # | Requirement | Rationale |
|---|---|---|
| M-1 | Load generation must be **open-loop**: messages are sent on a schedule derived from a target rate, and latency is measured from *intended* send time, not actual send time | A closed-loop generator suffers coordinated omission. When the broker stalls, the generator stops sending, so the stall never enters the latency distribution. This single error is responsible for most optimistic published benchmarks. |
| M-2 | Latency must be recorded in an **HdrHistogram**, with percentiles computed from merged histograms | Percentiles cannot be averaged. The mean of per-second p99 values is meaningless. |
| M-3 | Every run records an **environment fingerprint**: CPU model and count, available RAM, kernel, Docker version, and the resolved image digest of every container | Numbers without an environment are not results. Digests, not tags, because tags are mutable. |
| M-4 | Brokers are **destroyed and recreated** between runs, including their volumes | Otherwise run N inherits run N-1 page cache, segment layout, and compaction debt. |
| M-5 | Each run has an explicit **warm-up period whose samples are discarded**, and the discard duration is recorded | JIT compilation and cache warming otherwise contaminate the head of the distribution. |
| M-6 | The **client library is part of the measurement**, and this is stated rather than hidden | Each broker is used through its own client. A "pure broker" number is not something an application can ever observe. What is measured is broker plus client as a deployed system. |
| M-7 | Configuration equivalence between the two systems must be **documented and justified per parameter**, including where exact equivalence is impossible | Kafka `acks=all` and Pulsar ack-quorum are similar but not identical. Pretending otherwise is the second most common benchmarking error. |
| M-8 | A run must be marked **invalid** if the generator could not sustain its target rate, and invalid runs must not be silently reported | Otherwise a throughput ceiling is misread as a latency result. |
| M-9 | Every published claim must name the config file that produced it | Traceability from claim to command. |

## 6. Functional requirements

### 6.1 Harness (the measurement instrument)

- **FR-1** Drive load against Kafka or Pulsar through a common driver interface, with all workload logic shared and only transport-specific code differing.
- **FR-2** Accept a declarative run configuration: message size, target rate, duration, warm-up, partitions, durability, batching, compression, key cardinality, producer and consumer counts.
- **FR-3** Generate payloads carrying an embedded monotonic send timestamp and sequence number.
- **FR-4** Detect and report gaps, duplicates, and reordering observed by the consumer.
- **FR-5** Emit a run manifest: config, environment fingerprint, validity flag, and summary statistics.
- **FR-6** Run a single benchmark from one command, with no manual broker setup.

### 6.2 Results storage

- **FR-7** Persist every run to Iceberg tables backed by Parquet on object storage.
- **FR-8** Model results as at minimum `bench.runs` (one row per run: config, environment, summary) and `bench.latency_samples` (histogram buckets).
- **FR-9** Never mutate or delete a completed run. Corrections are appended as new runs carrying a supersedes reference.
- **FR-10** Support schema evolution as new parameters are added, without rewriting historical runs.

### 6.3 Stream processing

- **FR-11** A Flink job consumes from either broker, applies the same downstream logic, and writes to Iceberg.
- **FR-12** Capture pipeline-level metrics: checkpoint duration and size, backpressure, sustained sink throughput, restart count.

### 6.4 Analysis

- **FR-13** PySpark jobs compute merged-histogram percentiles, throughput curves, and Kafka-versus-Pulsar comparisons for a given sweep.
- **FR-14** Detect run-over-run regressions against a named baseline sweep.
- **FR-15** Produce publication-ready charts and tables into `docs/results/`.

### 6.5 Orchestration

- **FR-16** An Airflow DAG executes a parameter sweep, enforcing teardown and rebuild between runs (M-4), and recording partial results if the sweep aborts.
- **FR-17** A maintenance DAG performs Iceberg compaction, snapshot expiry, and orphan-file cleanup on a schedule.

## 7. Non-functional requirements

| # | Requirement |
|---|---|
| NFR-1 | The entire stack runs on a single developer machine with 16 GB RAM, using Compose profiles so that no more than one broker and one engine need be resident at a time. |
| NFR-2 | All images are free and publicly pullable from Docker Hub or GHCR. No paid tiers, no licence keys. Bitnami images are prohibited, their free catalogue having been restricted in 2025. |
| NFR-3 | Cold start to a first completed benchmark run is documented and achievable in under 30 minutes on a clean machine. |
| NFR-4 | Every dependency and container image is version-pinned, with image digests recorded per run. |
| NFR-5 | The harness's own overhead is measured and reported, so readers can judge its contribution to the numbers. |
| NFR-6 | Repeat runs of an identical config must agree within a documented variance band. Runs outside it are flagged. |

## 8. Success criteria

The project is successful when all of the following hold.

1. A third party can clone the repo, run one documented command, and obtain a valid benchmark run.
2. At least one complete parameter sweep is published for both brokers, with methodology and environment stated.
3. Repeat runs of a fixed config land inside the documented variance band.
4. `docs/METHODOLOGY.md` addresses every requirement in section 5 explicitly.
5. The Flink to Iceberg to PySpark path works for both brokers.
6. A knowledgeable sceptic reading the methodology can identify what the results do and do not support, because the limitations are stated before anyone else has to point them out.

## 9. Risks and mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| The Python harness becomes the bottleneck before either broker does | High: measures the harness, not the brokers | Native clients (`confluent-kafka`, `pulsar-client`) that release the GIL; NFR-5 measures harness overhead; M-8 invalidates rate-starved runs |
| Config equivalence is disputed | High: invalidates the comparison | M-7 equivalence table published for review; run alternative settings rather than argue for one |
| Single-machine results over-generalised by readers | Medium: reputational | Section 4.2 exclusion restated in the README and in every published result |
| Local resource exhaustion produces noisy results | Medium | Compose profiles; explicit CPU and memory limits per container, recorded in the fingerprint |
| Scope creep into a third or fourth broker | Medium: nothing ships | Explicitly out of scope until the success criteria are met |
| Version drift across Flink, Spark, Iceberg, and Scala runtimes | Medium: build breakage | Pinned matrix in a single source of truth; an ADR recording the chosen versions |

## 10. Open questions

| # | Question | Needed by |
|---|---|---|
| Q-1 | Pulsar standalone, or a multi-bookie deployment for a fairer durability comparison against Kafka replication? | Milestone 3 |
| Q-2 | Is a Java Flink job required, or does Flink SQL suffice for the pipeline stage? | Milestone 5 |
| Q-3 | Which sweep becomes the canonical published baseline? | Milestone 8 |
