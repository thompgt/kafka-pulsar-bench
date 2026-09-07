"""Tests for SampleBuffer, manifest persistence, and driver registry."""

from __future__ import annotations

import pathlib

import pytest

from kpbench.config import ProducerConfig, RunConfig, TopicConfig, WorkloadConfig
from kpbench.drivers.registry import AVAILABLE, build_driver
from kpbench.metrics.samples import SampleBuffer
from kpbench.results import manifest as manifest_mod
from kpbench.workload.runner import RunOutcome


def _make_config(driver: str = "loopback") -> RunConfig:
    return RunConfig(
        name="test-manifest",
        driver=driver,
        workload=WorkloadConfig(
            message_bytes=64,
            target_rate_hz=1000,
            duration_s=1,
            warmup_s=0,
        ),
        topic=TopicConfig(name="test-topic", partitions=1),
        producer=ProducerConfig(),
    )


class TestSampleBuffer:
    def test_add_and_to_recorder(self) -> None:
        buf = SampleBuffer(expected_samples=10)
        assert buf.n == 0
        assert buf.overflow == 0

        # Add 5 samples
        for i in range(5):
            buf.add(recv_ns=1000 + i * 100, intended_ns=i * 100, send_ns=50 + i * 100)

        assert buf.n == 5
        assert buf.overflow == 0

        rec = buf.to_recorder()
        assert rec.count == 5
        assert rec.response.get_total_count() == 5
        assert rec.service.get_total_count() == 5

    def test_overflow_tracking(self) -> None:
        # Minimum capacity is 1024
        buf = SampleBuffer(expected_samples=1)
        assert buf._capacity == 1024

        # Exceed capacity
        for i in range(1025):
            buf.add(recv_ns=i, intended_ns=i, send_ns=i)

        assert buf.n == 1024
        assert buf.overflow == 1

    def test_throughput_per_second(self) -> None:
        buf = SampleBuffer(expected_samples=10)
        origin = 1_000_000_000

        # 2 samples in second 0, 3 samples in second 1
        buf.add(recv_ns=origin + 100_000_000, intended_ns=0, send_ns=0)
        buf.add(recv_ns=origin + 500_000_000, intended_ns=0, send_ns=0)
        buf.add(recv_ns=origin + 1_200_000_000, intended_ns=0, send_ns=0)
        buf.add(recv_ns=origin + 1_500_000_000, intended_ns=0, send_ns=0)
        buf.add(recv_ns=origin + 1_900_000_000, intended_ns=0, send_ns=0)

        tp = buf.throughput_per_second(origin)
        assert tp == {0: 2, 1: 3}


class TestManifest:
    def test_build_write_read_roundtrip(self, tmp_path: pathlib.Path) -> None:
        cfg = _make_config("loopback")
        outcome = RunOutcome(
            run_id="run-123",
            valid=True,
            reasons=[],
            metrics={"achieved_rate_hz": 1000.0},
        )
        env = {"host": {"platform": "Linux"}}
        client_info = {"driver": "loopback"}

        doc = manifest_mod.build(outcome, cfg, env, client_info)
        assert doc["manifest_version"] == manifest_mod.MANIFEST_VERSION
        assert doc["run_id"] == "run-123"
        assert doc["valid"] is True
        assert doc["driver"] == "loopback"

        written_path = manifest_mod.write(doc, tmp_path)
        assert written_path.exists()
        assert written_path.name == "manifest.json"

        loaded = manifest_mod.read(written_path)
        assert loaded == doc

    def test_read_invalid_json(self, tmp_path: pathlib.Path) -> None:
        p = tmp_path / "bad.json"
        p.write_text("[1, 2, 3]", encoding="utf-8")
        with pytest.raises(ValueError, match="must be a JSON object"):
            manifest_mod.read(p)


class TestRegistry:
    def test_available_drivers(self) -> None:
        assert set(AVAILABLE) == {"loopback", "kafka", "pulsar"}

    def test_build_driver_loopback(self) -> None:
        cfg = _make_config("loopback")
        driver = build_driver(cfg)
        assert driver.name == "loopback"

    def test_build_driver_unknown_raises(self) -> None:
        cfg = _make_config("redis")
        with pytest.raises(ValueError, match="unknown driver 'redis'"):
            build_driver(cfg)
