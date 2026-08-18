"""Driver lookup.

Imports are deferred so that a missing optional client library only breaks the
driver that needs it. Someone benchmarking Kafka should not need the Pulsar
client installed, and vice versa.
"""

from __future__ import annotations

from kpbench.config import RunConfig
from kpbench.drivers.base import Driver

AVAILABLE = ("loopback", "kafka", "pulsar")


def build_driver(config: RunConfig) -> Driver:
    name = config.driver.lower()

    if name == "loopback":
        from kpbench.drivers.loopback import LoopbackDriver

        return LoopbackDriver(config)

    if name == "kafka":
        from kpbench.drivers.kafka_driver import KafkaDriver

        return KafkaDriver(config)

    if name == "pulsar":
        try:
            from kpbench.drivers.pulsar_driver import PulsarDriver
        except ImportError as exc:  # pragma: no cover - depends on install extras
            raise RuntimeError(
                "pulsar driver unavailable; install the 'pulsar' extra"
            ) from exc
        return PulsarDriver(config)

    raise ValueError(f"unknown driver {config.driver!r}; expected one of {AVAILABLE}")
