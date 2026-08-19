# ADR-0003: Benchmark runs execute under Linux, never on the Windows host

**Status:** Accepted
**Date:** 2026-08-18
**Milestone:** M2
**Relates to:** ADR-0002, NFR-3

## Context

`CLAUDE.md` already advised running the stack from WSL2, on the grounds that
bind-mount performance across the Windows filesystem boundary is poor. That
advice turned out to understate the problem, and for a different reason.

While chasing a Kafka smoke run that reported far higher latency than the raw
round-trip time to the same broker, the consumer's poll loop was measured
directly. On an **idle topic, on the main thread, with no other threads in the
process**, a `consume()` call requesting a 5 ms timeout took:

| Host | p50 | p90 | max |
|---|---|---|---|
| Windows (Git Bash, host CPython 3.12.5) | 15.47 ms | 16.70 ms | 21.38 ms |
| WSL2 Ubuntu 24.04 (CPython 3.12.3) | 5.25 ms | 5.36 ms | 6.33 ms |

Windows quantises the wait to roughly 15.625 ms — the default system scheduler
tick. Linux honours the requested timeout. Identical harness code, identical
broker container, identical machine.

`timeBeginPeriod(1)` was tried and did not help, so this is not fixable from
inside the process.

The end-to-end effect on the smoke workload at 2,000/s was a p50 of ~83 ms on
the Windows host against ~26 ms under WSL2.

## Decision

**No benchmark result produced on the Windows host is valid.** Runs execute
under WSL2 or on native Linux.

This is a correctness rule, not a performance preference. A 15.6 ms floor on
every poll is larger than the broker latencies being measured, so a Windows run
does not measure the broker at all — it measures the Windows scheduler, while
producing numbers that look entirely plausible.

Windows remains fine for editing, running the unit tests, and driving Compose.

## Consequences

- The floor figures in ADR-0002 were taken on the Windows host and are
  therefore **pessimistic**. They must be re-measured under WSL2 before any
  result is published, and the operating envelope revisited — the supported
  rate ceiling may be higher than 10k/s once the platform artifact is removed.
- The harness should refuse to run, or at minimum warn loudly, when it detects
  a Windows host. `preflight.sh` already warns; the harness itself should carry
  the same check so that a run started directly cannot silently produce an
  invalid result.
- Reproduction instructions must state the platform requirement prominently
  rather than as a footnote.
- CI, if added, must run benchmarks on Linux.

## Why this was worth finding

This is the third defect in a row at this milestone that produced a
plausible-looking number instead of an error: continuous busy-spin, histograms
in the hot path, and a poll timeout that capped consumer throughput. All three
would have survived into published results had the harness not been measured
against itself first.

It is also the clearest justification so far for the project's central claim —
that a benchmark's methodology matters more than its numbers. A reasonable
person would have run this on Windows, got self-consistent results, and
published them.
