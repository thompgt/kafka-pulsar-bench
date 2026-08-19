"""Pulsar driver, built on the official pulsar-client (C++ core with Python bindings).

Symmetry with the Kafka driver is the requirement here, not idiomatic Pulsar
usage (invariant 3). Where the two systems genuinely cannot be made equivalent,
the asymmetry is named in a comment and carried into the equivalence table in
`docs/METHODOLOGY.md` — never quietly smoothed over, because a tidy table that
overstates equivalence is worse than an honest one that admits a gap.

Two settings here are chosen specifically to avoid repeating defects found in
M2, and both are marked below: the batch-receive timeout (which defaults to
100 ms and would cap consumer throughput exactly as the Kafka poll timeout did)
and `block_if_queue_full` (which would turn the open loop into a closed one).
"""

from __future__ import annotations

import contextlib
import json
import time
import urllib.error
import urllib.request
from typing import Any, ClassVar

import pulsar

from kpbench.config import Compression, Durability, RunConfig
from kpbench.drivers.base import Driver, DriverError

# Kafka gzip maps to Pulsar ZLIB. They are both DEFLATE-based but not the same
# framing, so a gzip-vs-ZLIB comparison is approximate. Flagged in the table.
_COMPRESSION = {
    Compression.NONE: pulsar.CompressionType.NONE,
    Compression.LZ4: pulsar.CompressionType.LZ4,
    Compression.ZSTD: pulsar.CompressionType.ZSTD,
    Compression.SNAPPY: pulsar.CompressionType.SNAPPY,
    Compression.GZIP: pulsar.CompressionType.ZLib,
}


class PulsarDriver(Driver):
    name: ClassVar[str] = "pulsar"

    def __init__(self, config: RunConfig) -> None:
        super().__init__(config)
        opts = config.driver_options
        self._service_url = opts.get("service.url", "pulsar://localhost:6650")
        self._admin_url = opts.get("admin.url", "http://localhost:8080")
        self._tenant = opts.get("tenant", "public")
        self._namespace = opts.get("namespace", "default")
        self._subscription = opts.get("subscription", f"kpbench-{config.name}")

        self._client: pulsar.Client | None = None
        self._producer: pulsar.Producer | None = None
        self._consumer: pulsar.Consumer | None = None
        self._pending = 0
        self._send_errors = 0
        self._last_msg: Any = None

    @property
    def _topic(self) -> str:
        return f"persistent://{self._tenant}/{self._namespace}/{self.config.topic.name}"

    # --- admin over REST -------------------------------------------------
    # The Python client has no admin API, so topic lifecycle goes through the
    # broker's REST interface.
    def _admin(self, method: str, path: str, body: str | None = None) -> str:
        url = f"{self._admin_url}/admin/v2/{path}"
        data = body.encode("utf-8") if body is not None else None
        req = urllib.request.Request(url, data=data, method=method)
        if data is not None:
            req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return str(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise DriverError(f"{method} {url} -> {exc.code}: {detail}") from exc
        except urllib.error.URLError as exc:
            raise DriverError(f"{method} {url} failed: {exc.reason}") from exc

    def _topic_path(self) -> str:
        return f"persistent/{self._tenant}/{self._namespace}/{self.config.topic.name}"

    def provision(self) -> None:
        # Partitioned even at partitions=1, so that the topic type is identical
        # across the sweep. A non-partitioned topic behaves differently enough
        # that mixing the two would confound partition-count comparisons.
        self._admin("PUT", f"{self._topic_path()}/partitions", str(self.config.topic.partitions))
        self._await_metadata(self.config.topic.partitions)

    def _await_metadata(self, partitions: int, timeout_s: float = 30.0) -> None:
        """Wait for the partition metadata to be visible.

        Same reasoning as the Kafka driver: producing before metadata settles
        records setup cost as broker latency.
        """
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            try:
                body = self._admin("GET", f"{self._topic_path()}/partitions")
                if int(json.loads(body).get("partitions", 0)) == partitions:
                    return
            except DriverError:
                pass
            time.sleep(0.2)
        raise DriverError(f"{self._topic} metadata did not settle within {timeout_s}s")

    def deprovision(self) -> None:
        if not self.config.topic.delete_after_run:
            return
        # Teardown must never fail a run: a topic that cannot be deleted is a
        # cleanup problem, not a reason to discard measurements already taken.
        with contextlib.suppress(DriverError):
            self._admin("DELETE", f"{self._topic_path()}/partitions?force=true")

    # --- client ----------------------------------------------------------
    def _ensure_client(self) -> pulsar.Client:
        if self._client is None:
            self._client = pulsar.Client(self._service_url, operation_timeout_seconds=30)
        return self._client

    # --- producer --------------------------------------------------------
    def start_producer(self) -> None:
        p = self.config.producer
        self._producer = self._ensure_client().create_producer(
            self._topic,
            compression_type=_COMPRESSION[p.compression],
            # Kafka batches opportunistically even at linger.ms=0; Pulsar
            # batching is left enabled to match, with the delay carried over.
            # The internal trigger conditions are not identical - see the
            # equivalence table.
            batching_enabled=True,
            batching_max_publish_delay_ms=max(1, int(p.linger_ms)),
            batching_max_allowed_size_in_bytes=p.batch_max_bytes,
            batching_max_messages=1_000_000,
            max_pending_messages=1_000_000,
            max_pending_messages_across_partitions=2_000_000,
            # MUST stay False. Blocking to make room would make the producer
            # wait on the broker, closing the open loop and reintroducing
            # coordinated omission (invariant 1). The Kafka driver raises on a
            # full queue for the same reason.
            block_if_queue_full=False,
            send_timeout_millis=30_000,
        )

    def _on_send(self, res: Any, msg_id: Any) -> None:
        self._pending -= 1
        if res != pulsar.Result.Ok:
            self._send_errors += 1

    def send(self, key: bytes | None, value: bytes) -> None:
        if self._producer is None:
            raise DriverError("producer not started")
        try:
            # Async, like the Kafka driver: send() must never wait for an
            # acknowledgement, or the schedule stops being open-loop.
            self._producer.send_async(
                value,
                self._on_send,
                partition_key=key.decode("ascii") if key is not None else None,
            )
            self._pending += 1
        except Exception as exc:
            raise DriverError(
                f"pulsar send failed ({exc}); the harness cannot sustain this rate"
            ) from exc

    def flush(self, timeout_s: float) -> int:
        if self._producer is None:
            return 0
        deadline = time.monotonic() + timeout_s
        with contextlib.suppress(Exception):
            self._producer.flush()
        while self._pending > 0 and time.monotonic() < deadline:
            time.sleep(0.01)
        return max(0, self._pending)

    # --- consumer --------------------------------------------------------
    def start_consumer(self) -> None:
        c = self.config.consumer
        self._consumer = self._ensure_client().subscribe(
            self._topic,
            subscription_name=self._subscription,
            consumer_type=pulsar.ConsumerType.Exclusive,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=c.max_poll_records,
            max_total_receiver_queue_size_across_partitions=c.max_poll_records * 10,
            # The default batch-receive policy has a 100ms timeout. Left alone
            # it would cap consumer throughput and inject queueing latency in
            # exactly the way the Kafka poll timeout did in M2. Both halves are
            # pinned to the same config values the Kafka driver uses.
            batch_receive_policy=pulsar.ConsumerBatchReceivePolicy(
                max_num_message=c.max_poll_records,
                max_num_bytes=-1,
                timeout_ms=c.poll_timeout_ms,
            ),
        )

    def wait_consumer_ready(self, timeout_s: float) -> bool:
        # Pulsar's subscribe() is synchronous: it returns once the subscription
        # exists on every partition, so there is no rebalance to wait for. This
        # is a genuine asymmetry with Kafka, where assignment is asynchronous
        # and must be awaited. Recorded in the equivalence table.
        return self._consumer is not None

    def poll(self, timeout_s: float) -> list[bytes]:
        if self._consumer is None:
            raise DriverError("consumer not started")
        try:
            msgs = self._consumer.batch_receive()
        except Exception as exc:
            if "Timeout" in str(exc) or "TimeOut" in str(exc):
                return []
            raise DriverError(f"pulsar receive error: {exc}") from exc

        out: list[bytes] = []
        last = None
        for m in msgs:
            out.append(bytes(m.data()))
            last = m
        if last is not None:
            # Cumulative rather than per-message: acknowledging individually
            # would put N extra client calls in the consumer hot path, which
            # the Kafka side does not pay. Pulsar requires acknowledgement to
            # avoid redelivery at ack timeout, so it cannot simply be skipped;
            # cumulative is the cheapest form. Noted as an asymmetry.
            self._consumer.acknowledge_cumulative(last)
            self._last_msg = last
        return out

    # --- teardown --------------------------------------------------------
    def close(self) -> None:
        for obj in (self._consumer, self._producer, self._client):
            if obj is not None:
                with contextlib.suppress(Exception):
                    obj.close()
        self._consumer = None
        self._producer = None
        self._client = None

    def client_info(self) -> dict[str, str]:
        return {
            "driver": self.name,
            "client": "pulsar-client",
            "client_version": getattr(pulsar, "__version__", "unknown"),
            "durability_note": (
                f"config durability={self.config.producer.durability.value}; "
                "Pulsar persistence is a namespace-level quorum setting, not a "
                "per-produce ack level - see the equivalence table"
            ),
        }


# Durability is deliberately not mapped to a producer option here.
#
# Kafka expresses it per-produce (acks=0/1/all). Pulsar expresses it as the
# namespace's ensemble/write/ack quorum, set administratively. In standalone
# mode with one bookie all three are necessarily 1, so Durability.LEADER and
# Durability.ALL collapse to the same physical behaviour, and there is no
# analogue of acks=0 at all.
#
# Pretending otherwise by inventing a mapping would be the single most
# misleading thing this driver could do, so it does not. Resolving it properly
# requires the multi-bookie deployment tracked as Q-1.
_DURABILITY_IS_NAMESPACE_LEVEL = (Durability.NONE, Durability.LEADER, Durability.ALL)
