# ADR-0002: Harness operating envelope

**Status:** Accepted, revised 2026-08-18 after ADR-0003
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

The measurement path was first exercised on the Windows host. Those numbers are
retained at the end of this ADR because they are what drove the initial
decision, but they are **not the operating envelope**: ADR-0003 established that
Windows quantises poll waits to the ~15.6 ms scheduler tick, so they measured
the platform rather than the harness.

The authoritative figures are taken under WSL2 (Ubuntu 24.04, CPython 3.12.3),
512-byte messages, single producer and consumer thread in one process. Thirteen
unrelated containers were running on the host, which is pessimistic relative to
a quiet machine.

| Target rate | Achieved | p50 | p99 | p99.9 | Verdict |
|---|---|---|---|---|---|
| 1,000/s | 100% | 163 us | 1.71 ms | 7.42 ms | clean |
| 5,000/s | 100% | 125 us | 1.27 ms | 8.81 ms | clean |
| 10,000/s | 100% | 175 us | 3.26 ms | 9.35 ms | clean |
| 20,000/s | 100% | 180 us | 2.64 ms | 6.47 ms | clean |
| 40,000/s | 100% | 282 us | 3.48 ms | 6.93 ms | clean |
| 60,000/s | 100% | 403 us | 6.06 ms | 9.39 ms | clean |
| 80,000/s | 100% | 496 us | 5.17 ms | 8.45 ms | usable |
| 100,000/s | 100% | 628 us | 4.47 ms | 7.53 ms | usable |
| 150,000/s | 100% | 1.78 ms | 87.8 ms | 94.7 ms | **collapse** |

Three defects were found and fixed before these numbers were taken. Each had
produced a plausible-looking result rather than an error, which is the failure
mode this project exists to guard against.

1. **Continuous busy-spin.** The scheduler's fixed 300 us spin window exceeded
   the send interval at any rate above ~3.3 kHz, so the producer spun without
   pause. A CPython busy-loop yields the GIL only every
   `sys.getswitchinterval()` — 5 ms by default — so the consumer thread waited
   milliseconds to be scheduled, and that wait was recorded as latency.
2. **Histograms in the hot path.** `hdrh` is pure Python; the consumer recorded
   three values per message. Above roughly 10k/s the consumer could not keep up
   and its own growing backlog appeared as broker latency. At 20k/s this alone
   produced a 255 ms p99.
3. **Poll timeout capping consumer throughput.** A batching `consume()` blocks
   for its whole timeout unless the batch fills, so a 100 ms timeout pinned
   consumer capacity near the production rate and made any startup deficit
   permanent. This one showed up against Kafka rather than loopback, as a 2.65 s
   p50 (see ADR-0003).

## Decision

**The harness operates at or below 100,000 messages/s per instance, on Linux.**

- **<= 60k/s** is the supported region. Floor: p50 ~200-400 us, p99 ~1.3-6 ms.
- **60k-100k/s** is usable with the floor reported alongside results.
- **> 100k/s** is not supported; the tail becomes the harness's own.
- **Windows hosts are rejected outright** by the validity gate (ADR-0003).

Any broker result whose p99 is within **3x of the floor at the same rate** must
be reported as harness-limited rather than as a broker measurement.

## Consequences

This is a real limitation on what the project can claim, and it is better stated
here than discovered by a reader.

- Throughput-ceiling comparisons remain **out of scope**, but are no longer
  absurd: 100k/s is within the range a single-node broker actually operates in.
  The project still measures *latency under controlled load* rather than maximum
  throughput, because at the ceiling it is the harness that saturates first.
- The p50 floor of ~200 us is comfortably below broker latencies and is not a
  concern. The **p99 floor of 1.3-6 ms is the same order as real Kafka p99**,
  which is the condition the workplan warned about. It does not invalidate the
  approach — the comparison of interest is Kafka against Pulsar under an
  identical floor, and a shared additive floor largely cancels in a difference —
  but it does mean **absolute** tail figures from this harness are not
  comparable with vendor-published numbers, and every published result must say
  so.
- Part of that p99 floor is host contention rather than the harness. It must be
  re-measured with `STRICT=1` on an idle machine before publication.
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

## Superseded measurements (Windows host)

Kept because they drove the original decision, and because the gap between the
two tables is the evidence for ADR-0003. Do not cite these as harness figures.

| Target rate | p50 | p99 | Verdict |
|---|---|---|---|
| 10,000/s | 355 us | 2.61 ms | clean |
| 20,000/s | 459 us | 6.23 ms | usable |
| 40,000/s | 443 us | 29.6 ms | degrading |
| 50,000/s | 556 us | 222 ms | invalid |

The Windows host collapsed at 50k/s where Linux is still clean at 100k/s — a
factor of ten, on identical code and hardware.

## Follow-up

- Re-measure on an idle host (`STRICT=1` in preflight) before publishing.
- Revisit whether separate producer/consumer processes are needed at all. At a
  100k/s ceiling the case is much weaker than it appeared on Windows.
