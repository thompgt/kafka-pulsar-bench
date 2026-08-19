"""Kafka driver, built on confluent-kafka (librdkafka).

The native client is not incidental. It releases the GIL during send and does
its batching and I/O on background C threads, which is what makes a Python
harness viable at benchmark rates at all. A pure-Python client would measure
Python.

Every tuning value here comes from the run config. Nothing is hardcoded,
because a setting that lives in code rather than config is invisible in the
manifest and therefore invisible in the published result.
"""

from __future__ import annotations

import contextlib
import time
from typing import Any, ClassVar

import confluent_kafka
from confluent_kafka import Consumer, KafkaError, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from kpbench.config import Compression, Durability, RunConfig
from kpbench.drivers.base import Driver, DriverError

_ACKS = {
    Durability.NONE: "0",
    Durability.LEADER: "1",
    Durability.ALL: "all",
}

_COMPRESSION = {
    Compression.NONE: "none",
    Compression.LZ4: "lz4",
    Compression.ZSTD: "zstd",
    Compression.SNAPPY: "snappy",
    Compression.GZIP: "gzip",
}


class KafkaDriver(Driver):
    name: ClassVar[str] = "kafka"

    def __init__(self, config: RunConfig) -> None:
        super().__init__(config)
        self._bootstrap = config.driver_options.get("bootstrap.servers", "localhost:29092")
        self._producer: Producer | None = None
        self._consumer: Consumer | None = None
        self._admin: AdminClient | None = None
        self._group_id = config.driver_options.get(
            "group.id", f"kpbench-{config.name}-{int(time.time())}"
        )

    # --- topic lifecycle -------------------------------------------------
    def _admin_client(self) -> AdminClient:
        if self._admin is None:
            self._admin = AdminClient({"bootstrap.servers": self._bootstrap})
        return self._admin

    def provision(self) -> None:
        topic = NewTopic(
            self.config.topic.name,
            num_partitions=self.config.topic.partitions,
            replication_factor=1,
        )
        futures = self._admin_client().create_topics([topic])
        for name, fut in futures.items():
            try:
                fut.result(timeout=30)
            except Exception as exc:
                if "already exists" in str(exc).lower():
                    continue
                raise DriverError(f"could not create topic {name}: {exc}") from exc

        # Creation returns before every broker knows about the new partitions.
        # Producing into that window yields spurious UNKNOWN_TOPIC errors and a
        # distorted first second, so wait for metadata to settle.
        self._await_metadata(self.config.topic.name, self.config.topic.partitions)

    def _await_metadata(self, topic: str, partitions: int, timeout_s: float = 30.0) -> None:
        deadline = time.monotonic() + timeout_s
        admin = self._admin_client()
        while time.monotonic() < deadline:
            md = admin.list_topics(topic=topic, timeout=5)
            t = md.topics.get(topic)
            if t is not None and t.error is None and len(t.partitions) == partitions:
                return
            time.sleep(0.2)
        raise DriverError(f"topic {topic} metadata did not settle within {timeout_s}s")

    def deprovision(self) -> None:
        if not self.config.topic.delete_after_run:
            return
        try:
            futures = self._admin_client().delete_topics([self.config.topic.name])
            for fut in futures.values():
                fut.result(timeout=30)
        except Exception:
            pass

    # --- producer --------------------------------------------------------
    def start_producer(self) -> None:
        p = self.config.producer
        conf: dict[str, Any] = {
            "bootstrap.servers": self._bootstrap,
            "acks": _ACKS[p.durability],
            "compression.type": _COMPRESSION[p.compression],
            "linger.ms": p.linger_ms,
            "batch.size": p.batch_max_bytes,
            "max.in.flight.requests.per.connection": p.max_in_flight,
            # Idempotence forces acks=all and caps in-flight, which would
            # silently override the durability the config asked for.
            "enable.idempotence": False,
            # The send buffer must be deep enough that enqueueing never blocks;
            # a blocking send would close the loop (invariant 1).
            "queue.buffering.max.messages": 2_000_000,
            "queue.buffering.max.kbytes": 2_097_151,
        }
        conf.update(
            {k: v for k, v in self.config.driver_options.items() if k.startswith("producer.")}
        )
        self._producer = Producer(
            {k.removeprefix("producer."): v for k, v in conf.items()}
        )

    def send(self, key: bytes | None, value: bytes) -> None:
        if self._producer is None:
            raise DriverError("producer not started")
        try:
            self._producer.produce(self.config.topic.name, value=value, key=key)
        except BufferError as exc:
            # Blocking to make room would be coordinated omission by the back
            # door. Fail loudly: the run is not measuring what it claims to.
            raise DriverError(
                "producer queue full; the harness cannot sustain this rate. "
                "Raise queue.buffering.max.messages or lower target_rate_hz."
            ) from exc
        # Serve delivery callbacks without blocking, so librdkafka's queues
        # drain while the schedule continues.
        self._producer.poll(0)

    def flush(self, timeout_s: float) -> int:
        if self._producer is None:
            return 0
        return int(self._producer.flush(timeout_s))

    # --- consumer --------------------------------------------------------
    def start_consumer(self) -> None:
        conf: dict[str, Any] = {
            "bootstrap.servers": self._bootstrap,
            "group.id": self._group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "fetch.wait.max.ms": self.config.consumer.fetch_max_wait_ms,
            # Without this the consumer waits to accumulate a minimum batch,
            # adding latency that belongs to the consumer, not the broker.
            "fetch.min.bytes": 1,
        }
        conf.update(
            {
                k.removeprefix("consumer."): v
                for k, v in self.config.driver_options.items()
                if k.startswith("consumer.")
            }
        )
        self._consumer = Consumer(conf)
        self._consumer.subscribe([self.config.topic.name])

    def wait_consumer_ready(self, timeout_s: float) -> bool:
        if self._consumer is None:
            raise DriverError("consumer not started")
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            # poll() drives the rebalance; assignment stays empty without it.
            self._consumer.poll(0.1)
            if self._consumer.assignment():
                return True
        return False

    def poll(self, timeout_s: float) -> list[bytes]:
        if self._consumer is None:
            raise DriverError("consumer not started")
        msgs = self._consumer.consume(
            num_messages=self.config.consumer.max_poll_records, timeout=timeout_s
        )
        out: list[bytes] = []
        for m in msgs:
            err = m.error()
            if err is not None:
                if err.code() == KafkaError._PARTITION_EOF:
                    continue
                raise DriverError(f"consume error: {err}")
            val = m.value()
            if val is not None:
                out.append(val)
        return out

    # --- teardown --------------------------------------------------------
    def close(self) -> None:
        if self._consumer is not None:
            with contextlib.suppress(Exception):
                self._consumer.close()
            self._consumer = None
        self._producer = None

    def client_info(self) -> dict[str, str]:
        return {
            "driver": self.name,
            "client": "confluent-kafka",
            "client_version": confluent_kafka.version()[0],
            "librdkafka_version": confluent_kafka.libversion()[0],
        }
