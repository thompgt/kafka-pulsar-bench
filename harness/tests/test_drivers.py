"""Tests for transport drivers, configuration translation, and symmetry invariants."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from kpbench.config import (
    Compression,
    Durability,
    ProducerConfig,
    RunConfig,
    TopicConfig,
    WorkloadConfig,
)
from kpbench.drivers.base import DriverError
from kpbench.drivers.kafka_driver import _ACKS, _COMPRESSION, KafkaDriver
from kpbench.drivers.loopback import LoopbackDriver
from kpbench.drivers.pulsar_driver import PulsarDriver


def _make_config(
    driver: str = "loopback",
    flush_messages: int | None = None,
    durability: Durability = Durability.LEADER,
    linger_ms: float = 0.0,
    batch_max_bytes: int = 16384,
    driver_options: dict[str, str] | None = None,
) -> RunConfig:
    return RunConfig(
        name="test-run",
        driver=driver,
        workload=WorkloadConfig(
            message_bytes=64,
            target_rate_hz=1000,
            duration_s=1,
            warmup_s=0,
        ),
        topic=TopicConfig(
            name="test-topic",
            partitions=3,
            flush_messages=flush_messages,
        ),
        producer=ProducerConfig(
            durability=durability,
            linger_ms=linger_ms,
            batch_max_bytes=batch_max_bytes,
        ),
        driver_options=driver_options or {},
    )


class TestLoopbackDriver:
    def test_lifecycle_and_transfer(self) -> None:
        cfg = _make_config("loopback")
        d = LoopbackDriver(cfg)
        d.provision()
        d.start_producer()
        d.start_consumer()
        assert d.wait_consumer_ready(1.0) is True

        d.send(None, b"msg-1")
        d.send(b"k1", b"msg-2")
        assert d.flush(1.0) == 0

        polled = d.poll(0.1)
        assert polled == [b"msg-1", b"msg-2"]
        info = d.client_info()
        assert info["driver"] == "loopback"
        d.close()
        d.deprovision()


class TestKafkaDriverConfig:
    def test_durability_and_compression_mappings(self) -> None:
        assert _ACKS[Durability.NONE] == "0"
        assert _ACKS[Durability.LEADER] == "1"
        assert _ACKS[Durability.ALL] == "all"
        assert _COMPRESSION[Compression.NONE] == "none"
        assert _COMPRESSION[Compression.LZ4] == "lz4"
        assert _COMPRESSION[Compression.ZSTD] == "zstd"

    def test_flush_messages_plumbed_to_new_topic(self, monkeypatch: pytest.MonkeyPatch) -> None:
        cfg = _make_config("kafka", flush_messages=1)
        driver = KafkaDriver(cfg)

        mock_admin = MagicMock()
        future_mock = MagicMock()
        future_mock.result.return_value = None
        mock_admin.create_topics.return_value = {cfg.topic.name: future_mock}
        driver._admin = mock_admin

        # Mock metadata settle
        monkeypatch.setattr(driver, "_await_metadata", lambda topic, parts, timeout_s=30: None)

        driver.provision()

        mock_admin.create_topics.assert_called_once()
        created_topics = mock_admin.create_topics.call_args[0][0]
        assert len(created_topics) == 1
        topic_obj = created_topics[0]
        assert topic_obj.topic == "test-topic"
        assert topic_obj.num_partitions == 3
        assert topic_obj.config == {"flush.messages": "1"}

    def test_delivery_error_surfaces_in_flush(self) -> None:
        cfg = _make_config("kafka")
        driver = KafkaDriver(cfg)
        mock_producer = MagicMock()
        mock_producer.flush.return_value = 0
        driver._producer = mock_producer

        # Simulate a delivery failure callback
        driver._on_delivery("delivery timed out", None)
        assert driver._send_errors == 1

        with pytest.raises(DriverError, match="kafka delivery failed for 1 messages"):
            driver.flush(1.0)


class TestPulsarDriverCorrectness:
    def test_per_partition_cumulative_ack(self) -> None:
        cfg = _make_config("pulsar")
        driver = PulsarDriver(cfg)

        mock_consumer = MagicMock()

        # Build mock messages from two different partition topics
        msg_p0_1 = MagicMock()
        msg_p0_1.data.return_value = b"p0-1"
        msg_p0_1.topic_name.return_value = "persistent://public/default/test-topic-partition-0"

        msg_p1_1 = MagicMock()
        msg_p1_1.data.return_value = b"p1-1"
        msg_p1_1.topic_name.return_value = "persistent://public/default/test-topic-partition-1"

        msg_p0_2 = MagicMock()
        msg_p0_2.data.return_value = b"p0-2"
        msg_p0_2.topic_name.return_value = "persistent://public/default/test-topic-partition-0"

        mock_consumer.batch_receive.return_value = [msg_p0_1, msg_p1_1, msg_p0_2]
        driver._consumer = mock_consumer

        payloads = driver.poll(0.1)
        assert payloads == [b"p0-1", b"p1-1", b"p0-2"]

        # Crucial assertion: acknowledge_cumulative MUST be called for each partition!
        # Specifically, for partition-0 it must acknowledge the later message (msg_p0_2),
        # and for partition-1 it must acknowledge msg_p1_1.
        assert mock_consumer.acknowledge_cumulative.call_count == 2
        acked_messages = [
            call[0][0] for call in mock_consumer.acknowledge_cumulative.call_args_list
        ]
        assert msg_p0_2 in acked_messages
        assert msg_p1_1 in acked_messages
        assert msg_p0_1 not in acked_messages  # p0-1 was superseded by p0-2

    def test_send_error_surfaces_in_flush(self) -> None:
        cfg = _make_config("pulsar")
        driver = PulsarDriver(cfg)
        mock_producer = MagicMock()
        driver._producer = mock_producer

        # Simulate failed async callback
        import pulsar

        driver._pending = 1
        driver._on_send(pulsar.Result.UnknownError, None)
        assert driver._send_errors == 1

        with pytest.raises(DriverError, match="pulsar send failed for 1 messages"):
            driver.flush(1.0)

    def test_batching_disabled_when_zero_linger_and_zero_batch(self) -> None:
        cfg = _make_config("pulsar", linger_ms=0, batch_max_bytes=0)
        driver = PulsarDriver(cfg)
        mock_client = MagicMock()
        driver._client = mock_client

        driver.start_producer()
        call_kwargs = mock_client.create_producer.call_args[1]
        assert call_kwargs["batching_enabled"] is False
