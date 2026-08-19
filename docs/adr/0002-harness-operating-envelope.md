# ADR-0002: Harness operating envelope

**Status:** Accepted
**Date:** 2026-08-18
**Milestone:** M2
**Supersedes:** nothing
**Relates to:** NFR-5, M-8

## Context

The workplan required the harness to measure itself before anything was built
on top of it, and stated the stopping condition plainly: *if the loopback floor
turns out to be close to expected broker latencies, reconsider the harness
language for the measurement path.*

The loopback driver runs the full measurement path — open-loop scheduling,
payload encoding, queue handoff, decode, sample capture, delivery tracking —
with no broker at all. Whatever it reports is a floor: no configuration of any
broker can be resolved below it, because the harness itself contributes that
much.

## Measurements

Windows 11, Docker VM 15 GB / 12 CPUs, 512-byte messages, single producer and
consumer thread in one CPython 3.12 process. Thirteen unrelated containers were
running on the host, which is representative of a developer machine and
pessimistic relative to a quiet one.

After the fixes described below:

| Target rate | Achieved | p50 | p99 | p99.9 | Verdict |
|---|---|---|---|---|---|
| 1,000/s | 100% | 304 us | 1.15 ms | 3.71 ms | clean |
| 2,000/s | 100% | 267 us | 1.07 ms | 4.99 ms | clean |
| 5,000/s | 100% | 392 us | 2.58 ms | 6.91 ms | clean |
| 10,000/s | 100% | 355 us | 2.61 ms | 6.36 ms | clean |
| 20,000/s | 100% | 459 us | 6.23 ms | 10.55 ms | usable |
| 30,000/s | 100% | 694 us | 23.2 ms | 30.3 ms | degrading |
| 40,000/s | 100% | 443 us | 29.6 ms | 34.7 ms | degrading |
| 50,000/s | 97.7% | 556 us | 222 ms | 230 ms | **invalid** |

Two defects were found and fixed before these numbers were taken. Both had
produced a plausible-looking result rather than an error, which is exactly the
failure mode this project exists to guard against.

1. **Continuous busy-spin.** The scheduler's fixed 300 us spin window exceeded
   the send interval at any rate above ~3.3 kHz, so the producer spun without
   pause. A CPython busy-loop yields the GIL only every
   `sys.getswitchinterval()` — 5 ms by default — so the consumer thread waited
   milliseconds to be scheduled and that wait was recorded as latency.
2. **Histograms in the hot path.** `hdrh` is pure Python; the consumer recorded
   three values per message. Above roughly 10k/s the consumer could not keep up
   and its own growing backlog appeared in the results as broker latency. At
   20k/s this alone produced a 255 ms p99.

## Decision

**The harness operates at or below 10,000 messages/s per instance.**

- **≤ 10k/s** is the supported region. Floor: p50 ~350 us, p99 ~2.6 ms.
- **10k–20k/s** is usable with the floor explicitly reported alongside results.
- **> 20k/s** is not supported. The tail is dominated by the harness.
- **≥ 50k/s** is rejected outright by the validity gate (M-8).

Any broker result whose p99 is within **3x of the floor at the same rate** must
be reported as harness-limited rather than as a broker measurement.

## Consequences

This is a real limitation on what the project can claim, and it is better stated
here than discovered by a reader.

- Throughput-ceiling comparisons between Kafka and Pulsar are **out of reach**
  at these rates. Both brokers handle far more than 10k/s on this hardware, so
  the harness saturates long before either does. This project measures *latency
  under controlled load*, not maximum throughput.
- The p99 floor of ~2.6 ms is the same order as real Kafka p99 latency. This is
  the condition the workplan warned about. It does not invalidate the approach,
  because the comparison of interest is Kafka against Pulsar under an identical
  floor, and a shared additive floor mostly cancels in a difference. But it does
  mean **absolute** latency figures from this harness are not comparable to
  vendor-published numbers, and every published result must say so.
- The floor must be re-measured whenever the measurement path changes, and on
  each machine that produces published results. It is not a constant.

## Alternatives considered

**Separate producer and consumer processes.** Removes GIL contention entirely
and is the obvious next step if the envelope proves too tight. Deferred rather
than rejected: it needs a cross-process monotonic clock (`time.monotonic_ns` is
system-wide on both Linux and Windows, so this is workable) and it complicates
the run lifecycle. Not worth doing before there is evidence that 10k/s is
insufficient for the comparisons the project actually wants to make.

**Rewriting the measurement path in a compiled language.** Would raise the
ceiling by an order of magnitude and remove the floor concern. Rejected for now
as disproportionate: it doubles the project's language surface to fix a limit
that only binds for throughput-ceiling work, which is already out of scope.

**Accepting the original numbers and reporting them.** Rejected. A 9.4 ms p50
floor would have made every subsequent broker measurement meaningless while
still looking like a result.

## Follow-up

- Re-measure the floor under WSL2 rather than Git Bash on Windows. The M1 notes
  already recommend WSL2 for benchmarking, and the tail here is likely worse
  than it needs to be because of Windows scheduling and host contention.
- Re-measure on an idle host (`STRICT=1` in preflight) before publishing.
