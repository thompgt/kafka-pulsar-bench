# ADR-0001: Version matrix

**Status:** Accepted
**Date:** 2026-08-18
**Milestone:** M1

## Context

The workplan flagged the version matrix in `CLAUDE.md` as provisional. Six
runtimes have to agree with one another (Flink and Spark each need an Iceberg
runtime jar built for their exact minor version and Scala version), and a
benchmark whose stack cannot be rebuilt identically is not reproducible.

A second concern is specific to this project: the two systems under test must be
matched in *maturity* as well as in configuration. Comparing a brand-new release
of one broker against a long-term-support release of the other would bias the
result before a single message is sent.

## Decision

| Component | Version | Image |
|---|---|---|
| Kafka | 4.2.1 | `apache/kafka` |
| Pulsar | 4.2.4 | `apachepulsar/pulsar` |
| Iceberg | 1.10.1 | `apache/iceberg-rest-fixture` |
| MinIO | RELEASE.2025-04-22T22-12-26Z | `minio/minio` |
| Flink | 1.20.5, Scala 2.12, Java 17 | `flink` |
| Spark | 3.5.6 | `spark:3.5.6-python3` |
| Airflow | 3.0.3 | `apache/airflow` |
| Postgres | 16-alpine | `postgres` |
| Python | 3.12 | harness |

Recorded in `infra/compose/versions.env`. Digests in `infra/compose/images.lock`.

### Rationale per choice

**Kafka 4.2.1 and Pulsar 4.2.4.** Both are the latest patch release of the
current stable minor line of their respective projects. This is the symmetric
choice: each broker gets a settled release rather than a bleeding-edge one, and
neither gets an LTS-versus-current advantage. Kafka 4.3.x was available and
rejected as too new to be settled; Pulsar 4.0.x LTS was available and rejected
because pairing an LTS release against a current release is exactly the
asymmetry this decision exists to avoid.

**Kafka 4.x means KRaft only.** ZooKeeper was removed in 4.0, which removes a
container and a tuning surface from the comparison.

**Flink 1.20 rather than 2.x.** Verified that `iceberg-flink-runtime-1.20`
version 1.10.1 is published on Maven Central. The 2.x connector line was not
verified and is not worth the risk on the critical path. Revisit at M5 (Q-2).

**Spark 3.5.6 with Scala 2.12.** Matches the published
`iceberg-spark-runtime-3.5_2.12:1.10.1`. Spark 4.0 images exist but the Iceberg
runtime pairing was not verified.

**Python 3.12, not 3.13.** The development machine defaults to 3.13; the harness
pins 3.12 because PySpark and some native client wheels still lag.

**No Bitnami images.** The free Bitnami catalogue was restricted in 2025. Every
image above is either an Apache project image or a Docker Official Image.

## Verification performed

- Every tag confirmed to exist via `docker manifest inspect`
- Every tag resolved to a multi-arch index digest and written to `images.lock`
- `iceberg-flink-runtime-1.20:1.10.1`, `iceberg-spark-runtime-3.5_2.12:1.10.1`,
  and `iceberg-aws-bundle:1.10.1` confirmed present on Maven Central (HTTP 200)

## Consequences

- A version bump invalidates cross-version benchmark comparison. Any bump needs
  a note in the affected results and, if a published baseline exists, a re-run.
- `postgres:16-alpine` is a floating tag. It is pinned by digest in the lock, and
  it serves only the Airflow metadata database, so it never touches the
  measurement path.
- `scripts/resolve-digests.sh --check` can gate CI to catch silent tag movement.

## Alternatives rejected

- **Latest of everything.** Maximises the chance of a broken Iceberg/engine
  pairing on the critical path, for no benchmarking benefit.
- **Kafka LTS vs Pulsar LTS.** Superficially symmetric, but the two projects do
  not define LTS the same way, so it is a less defensible pairing than
  latest-stable-minor on both sides.
