# kafka-pulsar-bench

A reproducible harness for comparing **Apache Kafka** and **Apache Pulsar** under
identical workloads, with results stored as versioned **Iceberg** tables and
analysed with **PySpark**.

The point of this project is not to declare a winner. It is to make the
comparison *honest and repeatable*: same load generator, same measurement
methodology, same hardware, environment fingerprinted on every run, and every
result reproducible from a committed config.

> **Status:** early scaffolding. No benchmark numbers have been produced yet, and
> nothing in this repo should be cited until `docs/METHODOLOGY.md` exists and
> `docs/results/` is populated.

## Stack

| Layer | Technology | Role |
|---|---|---|
| Transport under test | Kafka, Pulsar | The two systems being compared |
| Measurement | Python harness | Open-loop load generator, HdrHistogram latency capture |
| Stream processing | Flink | Realistic downstream consumer; pipeline-level metrics |
| Table format | Iceberg | Immutable, time-travellable snapshot of every run |
| File format | Parquet | Physical storage under Iceberg |
| Batch analysis | PySpark | Percentile aggregation, run-over-run regression detection |
| Orchestration | Airflow | Sweeps the parameter matrix, rebuilds brokers between runs |
| Object store | MinIO | Local S3 for the Iceberg warehouse |

## Repository layout

```
docs/          Requirements, workplan, methodology, ADRs, published results
infra/         Docker Compose profiles and broker configuration
harness/       Python load generator and measurement CLI (the instrument)
flink-jobs/    Flink pipeline consuming from either broker into Iceberg
analysis/      PySpark jobs over the results warehouse
airflow/       DAGs for sweep execution and Iceberg maintenance
scripts/       Developer entry points
```

## Documentation

- [Business requirements](docs/BUSINESS_REQUIREMENTS.md) — objectives, scope, success criteria
- [Workplan](docs/WORKPLAN.md) — milestones and task breakdown
- [Contributor guide](CLAUDE.md) — conventions and invariants

## Licence

MIT. See [LICENSE](LICENSE).
