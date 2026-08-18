"""Transport driver interface.

This is the only place broker-specific code is allowed to live (invariant 3).
Everything above it — scheduling, payload encoding, histogram capture,
validation, manifest writing — is shared, so that a difference in results
between Kafka and Pulsar can only come from the brokers and their clients, not
from two subtly different benchmark implementations.

The interface is deliberately narrow. Every method added here is a place where
the two drivers could diverge, so a new one needs a reason.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import ClassVar

from kpbench.config import RunConfig


class Driver(ABC):
    name: ClassVar[str]

    def __init__(self, config: RunConfig) -> None:
        self.config = config

    # --- topic lifecycle -------------------------------------------------
    @abstractmethod
    def provision(self) -> None:
        """Create the topic with the configured partition count.

        Explicit rather than relying on auto-creation, which would silently
        produce a single-partition topic and invalidate partition sweeps.
        """

    @abstractmethod
    def deprovision(self) -> None:
        """Remove the topic. Must not raise if it is already gone."""

    # --- producer --------------------------------------------------------
    @abstractmethod
    def start_producer(self) -> None: ...

    @abstractmethod
    def send(self, key: bytes | None, value: bytes) -> None:
        """Enqueue one message. Must not block on broker acknowledgement.

        Blocking here would turn the open loop into a closed one and reintroduce
        coordinated omission, so a driver that cannot enqueue must raise rather
        than wait.
        """

    @abstractmethod
    def flush(self, timeout_s: float) -> int:
        """Wait for outstanding sends. Returns the number still unsent."""

    # --- consumer --------------------------------------------------------
    @abstractmethod
    def start_consumer(self) -> None: ...

    @abstractmethod
    def wait_consumer_ready(self, timeout_s: float) -> bool:
        """Block until the consumer can actually receive.

        Kafka assigns partitions asynchronously after subscribe. Producing
        before assignment completes means the first messages sit unread and
        record enormous latency that belongs to the harness, not the broker.
        """

    @abstractmethod
    def poll(self, timeout_s: float) -> list[bytes]:
        """Return zero or more message payloads. Must not raise on timeout."""

    # --- teardown --------------------------------------------------------
    @abstractmethod
    def close(self) -> None:
        """Release client resources. Must be safe to call twice."""

    # --- provenance ------------------------------------------------------
    def client_info(self) -> dict[str, str]:
        """Client library identification, recorded in the manifest.

        Requirement M-6: the client is part of what is measured, so which
        client and which version must travel with the result.
        """
        return {"driver": self.name}


class DriverError(RuntimeError):
    pass
