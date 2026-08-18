"""In-process loopback driver: the harness measuring itself (NFR-5).

There is no broker here. Messages go into a queue and come straight back out,
so what this measures is scheduling precision, payload encoding, queue
handoff, and histogram recording — the harness's own floor.

This matters because the harness is written in Python. If its floor is close to
the latencies being reported for a real broker, then those numbers describe the
harness rather than the broker. Establishing the floor *before* building
anything on top of the measurement path is the whole point of running this
first; discovering it at the end would invalidate everything in between.
"""

from __future__ import annotations

import queue
import sys
from typing import ClassVar

from kpbench.drivers.base import Driver


class LoopbackDriver(Driver):
    name: ClassVar[str] = "loopback"

    def __init__(self, config: object) -> None:
        super().__init__(config)  # type: ignore[arg-type]
        # Unbounded on purpose. A bounded queue would block the producer when
        # full, which is exactly the closed-loop behaviour invariant 1 forbids.
        self._q: queue.SimpleQueue[bytes] = queue.SimpleQueue()
        self._closed = False

    def provision(self) -> None:
        return None

    def deprovision(self) -> None:
        return None

    def start_producer(self) -> None:
        return None

    def send(self, key: bytes | None, value: bytes) -> None:
        self._q.put(value)

    def flush(self, timeout_s: float) -> int:
        return 0

    def start_consumer(self) -> None:
        return None

    def wait_consumer_ready(self, timeout_s: float) -> bool:
        return True

    def poll(self, timeout_s: float) -> list[bytes]:
        out: list[bytes] = []
        try:
            out.append(self._q.get(timeout=timeout_s))
        except queue.Empty:
            return out
        # Drain whatever else is already queued; one item per poll would make
        # the consumer the bottleneck and measure the wrong thing.
        while True:
            try:
                out.append(self._q.get_nowait())
            except queue.Empty:
                break
        return out

    def close(self) -> None:
        self._closed = True

    def client_info(self) -> dict[str, str]:
        return {
            "driver": self.name,
            "python": sys.version.split()[0],
            "note": "no broker involved; establishes harness overhead floor",
        }
