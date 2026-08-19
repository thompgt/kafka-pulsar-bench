"""Run configuration.

A run config is the provenance record for every number this project publishes
(requirement M-9), so it is validated rather than trusted, and it is the only
place tuning values are allowed to live.
"""

from __future__ import annotations

import pathlib
from enum import StrEnum
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator


class Durability(StrEnum):
    """How many acknowledgements a producer waits for.

    The mapping to each broker's native setting is deliberately indirect: the
    config expresses *intent*, and each driver translates. Where the two
    systems cannot be made equivalent, the equivalence table records which way
    the bias runs (requirement M-7).
    """

    NONE = "none"
    """Fire and forget. Kafka acks=0."""
    LEADER = "leader"
    """Acknowledged by the receiving node only. Kafka acks=1."""
    ALL = "all"
    """Acknowledged by the full durability set. Kafka acks=all."""


class Compression(StrEnum):
    NONE = "none"
    LZ4 = "lz4"
    ZSTD = "zstd"
    SNAPPY = "snappy"
    GZIP = "gzip"


class PayloadFill(StrEnum):
    """What the message body is filled with.

    This is not cosmetic. Zero-filled payloads compress to almost nothing, so a
    compression sweep run against zeros measures nothing useful.
    """

    RANDOM = "random"
    ZERO = "zero"


class Base(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class WorkloadConfig(Base):
    message_bytes: int = Field(ge=32, le=10_000_000)
    target_rate_hz: float = Field(gt=0, le=5_000_000)
    duration_s: float = Field(gt=0, le=86_400)
    warmup_s: float = Field(ge=0, le=3_600)
    key_cardinality: int = Field(default=0, ge=0)
    """0 means send no key at all, which lets the broker round-robin."""
    payload_fill: PayloadFill = PayloadFill.RANDOM

    @property
    def total_messages(self) -> int:
        return int(self.target_rate_hz * (self.warmup_s + self.duration_s))

    @property
    def warmup_messages(self) -> int:
        return int(self.target_rate_hz * self.warmup_s)


class TopicConfig(Base):
    name: str = Field(min_length=1, max_length=200)
    partitions: int = Field(default=1, ge=1, le=1000)
    delete_after_run: bool = True


class ProducerConfig(Base):
    durability: Durability = Durability.LEADER
    compression: Compression = Compression.NONE
    linger_ms: float = Field(default=0.0, ge=0, le=60_000)
    batch_max_bytes: int = Field(default=16_384, ge=0)
    max_in_flight: int = Field(default=5, ge=1)


class ConsumerConfig(Base):
    fetch_max_wait_ms: int = Field(default=10, ge=0, le=60_000)
    """Low by default: a large fetch wait adds latency that is the consumer's
    doing, not the broker's, and would be misread as broker latency."""

    poll_timeout_ms: int = Field(default=5, ge=1, le=1_000)
    """How long a poll waits when nothing has arrived yet.

    Kept small deliberately. A batching consume call blocks for the whole
    timeout unless its batch fills, so a large value caps consumer throughput
    at roughly (batch / timeout) and adds up to the timeout to every message's
    measured latency. A 100ms value here once put 2.6s of harness queueing
    into results that looked like broker latency."""

    max_poll_records: int = Field(default=2_000, ge=1, le=100_000)
    """Batch ceiling per poll. Must be big enough that the consumer can burst
    ahead of the producer and drain a backlog, or any deficit it ever incurs
    becomes permanent."""


class ValidityConfig(Base):
    """Thresholds that decide whether a run may be reported at all (M-8)."""

    min_achieved_rate_ratio: float = Field(default=0.99, gt=0, le=1.0)
    """A generator that could not keep up measured its own ceiling, not the
    broker's latency."""
    max_missing_ratio: float = Field(default=0.0, ge=0, le=1.0)
    max_duplicate_ratio: float = Field(default=0.0, ge=0, le=1.0)
    drain_timeout_s: float = Field(default=30.0, gt=0)

    allow_windows_host: bool = False
    """Escape hatch for development only. Windows quantises poll waits to the
    ~15.6ms scheduler tick, which is larger than the latencies being measured,
    so a Windows run measures the scheduler rather than the broker while still
    producing plausible numbers. See ADR-0003."""


class RunConfig(Base):
    name: str = Field(min_length=1)
    driver: str = Field(min_length=1)
    """loopback | kafka | pulsar"""
    description: str = ""
    workload: WorkloadConfig
    topic: TopicConfig
    producer: ProducerConfig = ProducerConfig()
    consumer: ConsumerConfig = ConsumerConfig()
    validity: ValidityConfig = ValidityConfig()
    driver_options: dict[str, str] = Field(default_factory=dict)
    """Transport-specific escape hatch, e.g. bootstrap.servers. Anything here
    is recorded in the manifest so it cannot silently affect a result."""
    seed: int = 0

    @model_validator(mode="after")
    def _check_message_fits_header(self) -> RunConfig:
        from kpbench.workload.payload import HEADER_SIZE

        if self.workload.message_bytes < HEADER_SIZE:
            raise ValueError(
                f"message_bytes must be at least {HEADER_SIZE} to fit the "
                f"timing header, got {self.workload.message_bytes}"
            )
        return self

    @model_validator(mode="after")
    def _warn_on_unmeasurable_run(self) -> RunConfig:
        if self.workload.total_messages < 1:
            raise ValueError("target_rate_hz x duration yields zero messages")
        return self


def load_run_config(path: str | pathlib.Path) -> RunConfig:
    raw: Any = yaml.safe_load(pathlib.Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"{path}: expected a YAML mapping at the top level")
    return RunConfig.model_validate(raw)
