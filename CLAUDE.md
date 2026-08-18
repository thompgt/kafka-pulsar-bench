# CLAUDE.md — kafka-pulsar-bench

Guidance for Claude Code (and humans) working in this repository.

## What this project is

A reproducible harness comparing Apache Kafka and Apache Pulsar under identical
workloads. Results land in Iceberg tables, are analysed with PySpark, and sweeps
are orchestrated by Airflow.

**The deliverable is the harness and its methodology, not the numbers.** When a
change would make the benchmark faster to run but harder to trust, it is the
wrong change. See `docs/BUSINESS_REQUIREMENTS.md` section 5.

## Non-negotiable invariants

Breaking any of these silently invalidates every result the project has ever
produced. Treat a change that touches one as requiring an ADR.

1. **Open-loop load generation.** Latency is measured from the *intended* send
   time computed from the target rate, never from the actual send time. Never
   "wait for the previous send to complete before scheduling the next" — that is
   coordinated omission and it hides exactly the stalls the benchmark exists to find.
2. **Never average percentiles.** Merge HdrHistograms, then compute percentiles
   from the merged histogram. A mean of p99s is not a p99.
3. **Symmetry between drivers.** Any logic that is not transport-specific lives
   in shared code. If a fairness-relevant behaviour exists in one driver and not
   the other, the comparison is void. New driver features land in both or neither.
4. **Brokers are recreated between runs**, volumes included. No run inherits
   another's page cache or segment layout.
5. **Results are append-only.** Never `UPDATE` or `DELETE` a row in `bench.runs`.
   A correction is a new run that references the one it supersedes.
6. **Pin everything, record digests.** Image tags are mutable; digests are not.
   Every run records the resolved digest of every container involved.
7. **Rate-starved runs are invalid.** If the generator could not sustain its
   target rate, the run is flagged invalid and excluded from latency reporting.
   Do not quietly report it.

## Repository layout

```
docs/           Requirements, workplan, methodology, ADRs, published results
  adr/          Architecture decision records, NNNN-title.md
infra/compose/  One Compose file per concern, combined via profiles
infra/conf/     Broker and engine configuration, mounted into containers
harness/        Python measurement instrument (the core of the project)
  src/kpbench/drivers/    Transport adapters — the ONLY broker-specific code
  src/kpbench/workload/   Payload generation, open-loop rate scheduling
  src/kpbench/metrics/    HdrHistogram capture and merge
  src/kpbench/results/    Run manifests, Iceberg writes
  src/kpbench/env/        Environment fingerprinting
flink-jobs/     Flink pipeline consuming either broker into Iceberg
analysis/       PySpark jobs over the results warehouse
airflow/dags/   Sweep execution and Iceberg maintenance DAGs
scripts/        Developer entry points, invoked by the Makefile
results/        Local scratch for raw run output — gitignored
```

## Conventions

### Python

- Python 3.12 for the harness. **Not 3.13** — PySpark and some client wheels
  still lag. The version is pinned in `harness/pyproject.toml`.
- `ruff` for lint and format, `mypy --strict` on `harness/src`. Both run in CI.
- `pytest` for tests. The rate scheduler and histogram merge logic must have unit
  tests; they are where a subtle error would silently corrupt every result.
- Prefer stdlib and small dependencies in the harness. Every dependency added to
  the measurement path is a potential source of jitter.

### Configuration

- Run configs are YAML under `harness/configs/`, committed. A published result
  must name the config file that produced it.
- Never hardcode a broker address, topic name, or tuning value in Python. It goes
  in the config or in `infra/conf/`.

### Compose

- One file per concern; never one monolith. Bring up combinations with profiles:
  `core` (MinIO + Iceberg REST), `kafka`, `pulsar`, `flink`, `spark`, `airflow`.
- Every service declares explicit `cpus` and `mem_limit`. Unbounded containers
  make results non-comparable across runs.
- **Bitnami images are prohibited** — the free catalogue was restricted in 2025.
  Use `apache/*` and Docker Official images.

### Git

- Commit and push after each small logical unit, not at the end of a milestone.
- Conventional-ish subject lines in the imperative mood; explain *why* in the body.
- Work on `main` for scaffolding. Once benchmarking begins, changes to anything
  under `harness/src/kpbench/{workload,metrics,drivers}` go via a branch and PR,
  because they can invalidate historical comparability.

## Version matrix

**Provisional — to be verified and locked during Milestone 1**, then recorded in
`infra/compose/versions.env` as the single source of truth and in an ADR.

| Component | Intended version | Note |
|---|---|---|
| Kafka | 4.x | KRaft only; ZooKeeper removed as of 4.0 |
| Pulsar | 4.0.x LTS | |
| Flink | 1.20.x | Chosen over 2.x for mature Iceberg connector support — confirm before locking |
| Spark | 3.5.x | Iceberg runtime jars are per Spark+Scala version |
| Iceberg | 1.9.x | Separate runtime jar per engine |
| Airflow | 3.x | |
| Python | 3.12 | |

Do not bump any of these casually. A version change can shift benchmark numbers,
so it invalidates cross-version comparison and needs a note in the results.

## Running things

Entry points live in the Makefile. Until Milestone 1 lands, most of these do not
exist yet.

```bash
make up PROFILE=kafka        # bring up core + one broker
make bench CONFIG=configs/smoke.yaml
make down                    # tear down, volumes included
make sweep SWEEP=configs/sweeps/baseline.yaml
```

## Platform notes

Development happens on Windows. Both matter:

- **Run the stack from WSL2**, not native Windows. Bind-mount performance across
  the Windows filesystem boundary is bad enough to perturb measurements.
- **LF line endings are enforced** via `.gitattributes`. A CRLF shebang fails
  inside a Linux container with an opaque "bad interpreter" error.
- Cap the Docker VM's memory in `.wslconfig`. An uncapped VM will consume
  everything and add noise to results.
- Do not `set -u` before sourcing container or engine setup scripts; several
  reference unbound variables.
- **`export MSYS_NO_PATHCONV=1` before any `docker exec` with an absolute
  container path.** Git Bash rewrites `/opt/kafka/bin/...` into
  `C:/Program Files/Git/opt/kafka/bin/...` and the exec fails with a
  bewildering "no such file or directory".
- `make` is not installed in Git Bash. Every target is a thin wrapper over a
  script in `scripts/`, so `bash scripts/up.sh kafka` always works.
- Stop unrelated containers before a publishable run. `preflight.sh` warns when
  it finds them; `STRICT=1` promotes that to an error.

## Measured on this machine (M1, 2026-08-18)

Docker VM: 15 GB, 12 CPUs.

| Profile | Resident memory | Note |
|---|---|---|
| core (MinIO + Iceberg REST) | ~0.33 GB | always up |
| core + Kafka | ~0.78 GB | |
| core + Pulsar | ~1.75 GB | standalone runs ZK, bookie and broker in one JVM |

Cold start including image pull: ~3m20s for the Pulsar profile, the slower of
the two. Both are well inside the 8 GB exit criterion, which leaves room for
Flink or Spark alongside a broker.

## Where to look first

- Changing what is measured → `docs/BUSINESS_REQUIREMENTS.md` section 5, then an ADR
- Changing how it is measured → `harness/src/kpbench/workload/` and `metrics/`
- Adding a broker parameter → the config schema, both drivers, and the
  equivalence table in `docs/METHODOLOGY.md`
- Current priorities → `docs/WORKPLAN.md`
