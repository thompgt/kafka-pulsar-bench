# Methodology

**Status:** draft — the equivalence table (section 3) is complete enough to run
against, but Q-1 is unresolved and no results have been published yet.

This document exists so that a sceptic can work out what these results do and do
not support without having to read the code. Where the two systems cannot be
made equivalent, that is stated along with the direction of the resulting bias.

---

## 1. What is measured

**Response latency**: the interval from the time the open-loop schedule said a
message *should* have been sent, to the time the consumer received it. Not from
when the send call actually happened.

Two secondary series are recorded alongside it:

- **Service latency** — from actual send to receive. This is what a closed-loop
  benchmark reports. It is kept so the effect of coordinated omission can be
  shown from data rather than asserted.
- **Generator lag** — actual send minus intended send. Pure harness lateness. If
  this is not near zero the run is suspect regardless of what else it says.

Throughput is *not* a headline result. The harness saturates before either
broker does (ADR-0002), so this project measures latency under controlled load.

## 2. What is included in the number

The **client library is part of the measurement** and this is deliberate
(requirement M-6). Kafka is measured through `confluent-kafka`/librdkafka and
Pulsar through the official `pulsar-client`. There is no "pure broker" latency
that an application can ever observe; what is reported is broker plus client as
a deployed pair.

The **harness floor** is also in the number. Measured with a loopback driver
that removes the broker entirely: p50 ~200 us, p99 1.3–6 ms on Linux
(ADR-0002). Any result whose p99 is within 3x of the floor at the same rate is
reported as harness-limited, not as a broker measurement.

## 3. Configuration equivalence

The central fairness question. Each row states the Kafka setting, the Pulsar
setting, whether they are genuinely equivalent, and where they are not, which
system the difference favours.

### 3.1 Durability — the largest asymmetry

| | Kafka | Pulsar |
|---|---|---|
| Where it is set | per-produce (`acks`) | namespace quorum (ensemble / write / ack) |
| Default flush | **never fsyncs explicitly**; relies on the OS page cache | **fsyncs every write to the BookKeeper journal** (`journalSyncData=true`) |

**These defaults are not comparable.** Kafka with `acks=1` acknowledges once the
data is in the leader's page cache — durable against process death, *not*
against machine death. Pulsar's default acknowledges only after an fsync to the
journal — durable against machine death. Pulsar is doing strictly more work.

Measured cost of that difference, same workload (512 B, 2,000/s, 3 partitions,
warm broker, 3 discard runs first):

| Pulsar `journalSyncData` | p50 | p99 |
|---|---|---|
| `true` (default) | ~113 ms | 184–450 ms |
| `false` | ~58 ms | 118–153 ms |

Roughly a 2x latency cost for the stronger guarantee. A benchmark that compares
Kafka `acks=1` against stock Pulsar and reports "Kafka is faster" is measuring
this difference and calling it something else.

**Decision:** every durability comparison is run *both ways* — see ADR-0005.
Neither configuration is presented as the default.

**Unresolved (Q-1):** in standalone mode there is one bookie, so ensemble,
write quorum and ack quorum are all necessarily 1. `Durability.LEADER` and
`Durability.ALL` therefore collapse to identical physical behaviour, and there
is no analogue of `acks=0` at all. Until the multi-bookie deployment lands,
**durability sweeps are not meaningful on the Pulsar side** and must not be
published.

### 3.2 Other parameters

| Parameter | Kafka | Pulsar | Equivalent? |
|---|---|---|---|
| Partitioning | `partitions` on the topic | partitioned topic with N partitions | **Yes.** Harness always creates a partitioned topic, even at N=1, so the topic type never varies across a sweep. |
| Batching trigger | `linger.ms` | `batchingMaxPublishDelayMs` | **Approximately.** Both batch opportunistically, but Pulsar's minimum delay is 1 ms where Kafka accepts 0. The harness clamps Pulsar to `max(1, linger_ms)`, so a `linger=0` run gives Pulsar a 1 ms delay it did not ask for. **Favours Kafka** at low latency. |
| Batch size | `batch.size` | `batchingMaxAllowedSizeInBytes` | Yes. |
| Compression: lz4, zstd, snappy | native | native | Yes. |
| Compression: gzip | gzip | ZLib | **No.** Both DEFLATE-based, different framing. Approximate only; flag any gzip comparison. |
| Send blocking | raises `BufferError` when the queue is full | `block_if_queue_full=False` raises | Yes — both must raise rather than block, or the open loop closes (invariant 1). |
| In-flight limit | `max.in.flight.requests.per.connection` | `max_pending_messages` | **No.** Kafka's is per connection, Pulsar's is per producer. Not directly comparable; held constant rather than swept. |
| Consumer model | consumer group, 1 member | `Exclusive` subscription | **Approximately.** Both give one consumer all partitions. |
| Consumer readiness | assignment is **asynchronous**; must be awaited | `subscribe()` is **synchronous** | **No.** Kafka's initial rebalance costs ~3.2 s. Excluded from measurement on both sides by waiting for readiness before the schedule starts. |
| Offset start | `auto.offset.reset=earliest` | `InitialPosition.Earliest` | Yes. |
| Prefetch | `queued.min.messages` | `receiver_queue_size` | Approximately; both pinned to `max_poll_records`. |
| Poll batch | `consume(max_poll_records, poll_timeout)` | `ConsumerBatchReceivePolicy(max_num_message, timeout_ms)` | Yes — pinned to the same two config values. Both default badly (100 ms) and both are overridden. |
| Acknowledgement | `enable.auto.commit=false`, never commits | `acknowledge_cumulative` once per batch | **No.** Pulsar requires acknowledgement or messages redeliver at ack timeout; Kafka can simply never commit. **Favours Kafka** by one client call per batch. |
| Retention | `log.retention.ms` | `defaultRetentionTimeInMinutes` | Yes, both 60 min. |
| Auto topic creation | disabled | disabled | Yes — silent creation of a 1-partition topic would invalidate partition sweeps. |

### 3.3 Deployment asymmetry

Kafka runs as a single broker in KRaft mode: one process. Pulsar standalone runs
ZooKeeper, a bookie and a broker inside one JVM. These are not the same
deployment shape, and Pulsar's is doing more.

This is the substance of Q-1. It is disclosed rather than corrected, and it is
the strongest argument against over-reading any current Pulsar figure.

## 4. Procedure

1. `preflight.sh` — memory, disk, ports, foreign containers. `STRICT=1` for anything publishable.
2. Broker destroyed and recreated, volume included (invariant 4).
3. **Three discard runs** (ADR-0004). Broker warm-up spans runs, not just the in-run warm-up window.
4. Measured run(s), each on a fresh topic.
5. Validity gate applied before any reporting (M-8).

Every run records config, environment fingerprint, resolved image digests, and
client versions.

## 5. Known limitations

Stated here rather than left for a reader to find.

- **Single machine, single broker node.** Says nothing about distributed or
  cross-AZ behaviour, which is where much of the real difference between these
  systems lives.
- **Client libraries are included** in every number.
- **The harness floor** is p99 1.3–6 ms, the same order as real Kafka p99.
  Absolute tail figures are therefore not comparable with vendor-published
  numbers. The Kafka-vs-Pulsar *difference* is more defensible than either
  absolute value, because a shared additive floor largely cancels.
- **Neither broker is tuned.** Both run at documented, defensible settings, not
  at expert-tuned extremes. A specialist could likely improve either.
- **Pulsar standalone is not equivalent to single-node Kafka** (section 3.3).
- **Measurements to date were taken on a contended host** with unrelated
  containers running. Publishable runs require `STRICT=1`.
- **Pulsar's run-to-run variance is markedly higher than Kafka's** and does not
  settle after three discard runs the way Kafka's does. The uniform warm-up
  procedure is therefore itself a mild asymmetry, and the required number of
  discard runs should be established per broker.

## 6. Reproducing

```bash
bash scripts/preflight.sh              # STRICT=1 for publishable runs
bash scripts/reset-broker.sh kafka
# from WSL2 or Linux — never the Windows host (ADR-0003)
python -m kpbench.cli run harness/configs/smoke.yaml --warmup-runs 3
```

Every published claim names the config file that produced it (M-9).
