# ADR-0004: Variance band, and broker warm-up spanning runs

**Status:** Accepted
**Date:** 2026-08-18
**Milestone:** M2
**Relates to:** NFR-6, M-4, M-5

## Context

NFR-6 requires that repeat runs of an identical config agree within a
documented variance band, and that runs outside it are flagged. Establishing
that band means running the same config repeatedly and looking at the spread.

Ten repeats of `configs/smoke.yaml` (512 B, 2,000/s, 10 s, 3 s warm-up, 3
partitions, `acks=1`) were run against Kafka under WSL2. Each repeat used a
fresh topic. All ten were valid and all sustained 100% of target rate.

## What the data showed

Per-run p99, in order:

```
164  144  83  65  63  66  63  65  81  74   (ms)
```

| | n | median | min | max | spread | CV |
|---|---|---|---|---|---|---|
| p50, all runs | 10 | 17.3 ms | 13.5 | 22.4 | 1.66x | 17% |
| p99, all runs | 10 | 69.9 ms | 62.8 | 164.4 | 2.62x | 40% |
| p50, excluding first 3 | 7 | 14.8 ms | 13.5 | 21.8 | 1.62x | 17% |
| p99, excluding first 3 | 7 | 64.8 ms | 62.8 | 80.6 | **1.28x** | **9%** |

The first three runs after the broker started are systematically slower, and
the effect is large: excluding them cuts p99 variance from 40% to 9%.

This is broker warm-up — JIT compilation, page cache, allocation pools — and it
spans **runs**, not just the warm-up period inside a run. The 3 s in-run warm-up
required by M-5 handles topic and client warm-up. It does not handle this.

## Decision

**Documented variance band:** repeat runs of a fixed config on a warm broker
agree within **1.3x on p99** and **1.7x on p50**. A run outside that band
against a warm broker is flagged as anomalous.

**Broker warm-up procedure:** after any broker start or reset, run **three
discard runs** before the first measured run. Their manifests are written and
retained, marked as warm-up, and excluded from analysis.

## The tension this creates

Invariant 4 requires the broker to be destroyed and recreated between runs, so
that run N does not inherit run N-1's page cache and segment layout. Taken
alone, that puts every measured run in the cold, high-variance regime seen
above.

The two requirements are reconciled by making the warm-up explicit and equal:

1. Destroy and recreate the broker (invariant 4).
2. Run three discard runs (this ADR).
3. Run the measured run.

Every measured run therefore sits at the same point on the warm-up curve, which
is what makes runs comparable. The alternative — reusing a warm broker across a
sweep — would be cheaper and lower-variance, but would let each point inherit
the previous point's state, which is exactly the contamination invariant 4
exists to prevent. Comparability across points matters more than the absolute
variance of any single point.

## Consequences

- A sweep costs 4x its nominal run count in wall-clock time. This is a real
  cost and it lands on M7's DAG design.
- The discard runs are retained rather than deleted. They are evidence that the
  procedure was followed, and their spread is itself a signal: if a discard run
  is *faster* than the measured run that follows it, something is wrong.
- The band is specific to this hardware, this host contention level, and this
  config. It must be re-established for any published sweep, on the machine that
  produced it.
- These figures were taken with thirteen unrelated containers running. The band
  on an idle host (`STRICT=1`) is likely tighter and should be re-measured
  before publication.

## Follow-up

- Add a `--warmup-runs N` option to the CLI so the procedure is enforced by the
  tool rather than by discipline.
- Re-establish the band on an idle host.
- Check whether Pulsar's warm-up curve has the same shape at M3. If it needs
  more or fewer discard runs than Kafka, using the same number for both is
  itself an asymmetry, and belongs in the equivalence table.
