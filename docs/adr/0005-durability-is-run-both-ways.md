# ADR-0005: Durability comparisons are run both ways

**Status:** Accepted
**Date:** 2026-08-18
**Milestone:** M3
**Relates to:** M-7, Q-1

## Context

Requirement M-7 says configuration equivalence must be documented per parameter,
including where exact equivalence is impossible. Durability is the parameter
where it is impossible, and it is also the one that moves the numbers most.

The defaults are not comparable:

- **Kafka** acknowledges `acks=1` once the record is in the leader's page cache.
  It does not fsync explicitly. Durable against process death, not machine death.
- **Pulsar** acknowledges only after an fsync to the BookKeeper journal
  (`journalSyncData=true` by default). Durable against machine death.

Pulsar is doing strictly more work for its default acknowledgement.

Measured, same workload (512 B, 2,000/s, 3 partitions, warm broker, three
discard runs first):

| Pulsar `journalSyncData` | p50 | p99 |
|---|---|---|
| `true` (default) | ~113 ms | 184–450 ms |
| `false` | ~58 ms | 118–153 ms |

Roughly 2x. Large enough that whichever way it is resolved silently determines
the headline result.

## Decision

**Every durability comparison is run both ways, and both are published.**

- **Matched-guarantee:** Pulsar `journalSyncData=true` against Kafka configured
  to flush (`flush.messages=1`). Answers: *at equivalent durability, which is
  faster?*
- **Matched-default:** both systems at their shipped defaults. Answers: *if I
  install these and change nothing, what do I get?*

Neither is presented as *the* result. A published comparison that names only
one of these is incomplete, and the two answer genuinely different questions.

## Rationale

The alternatives were each worse in an obvious way.

**Pick matched-guarantee only.** Defensible on fairness grounds, but it reports
numbers nobody actually runs. Most Kafka deployments never enable per-message
flush.

**Pick matched-default only.** Reports what people actually run, but "Kafka is
2x faster than Pulsar" would then be largely a statement about fsync, presented
as a statement about the brokers. This is the specific error the project exists
to avoid.

**Pick one and disclose the other in a footnote.** The footnote does not travel.
The chart travels.

Running both costs twice the sweep time and produces a result that needs two
sentences instead of one. That is the correct trade.

## Consequences

- Sweep matrices double along the durability axis, compounding with the 4x from
  ADR-0004's warm-up procedure. M7's DAG must plan for it.
- The Kafka side needs `flush.messages=1` support plumbed through the config, so
  matched-guarantee is expressible. Not yet implemented.
- **Blocked on Q-1:** in standalone mode Pulsar has one bookie, so ensemble,
  write quorum and ack quorum are all 1. `Durability.LEADER` and
  `Durability.ALL` collapse to identical behaviour and there is no analogue of
  `acks=0`. Until a multi-bookie deployment exists, durability *sweeps* on the
  Pulsar side are not meaningful and must not be published. The fsync
  comparison above is still valid, because it does not depend on quorum size.
- Any chart carrying a durability comparison must state which mode it is, in the
  chart itself and not only in the caption.
