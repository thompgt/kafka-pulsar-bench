# results/

Scratch directory for raw benchmark output on the local machine. **Gitignored.**

Each run writes `results/<run_id>/` containing the run manifest, environment
fingerprint, and raw HdrHistogram files. The durable copy of every run lives in
the Iceberg warehouse (`bench.runs` / `bench.latency_samples`); this directory is
only a staging area before that load happens.

Curated results intended for publication belong in `docs/results/`, committed.
