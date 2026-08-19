"""Raw sample capture, kept out of the measurement hot path.

`hdrh` is a pure-Python HdrHistogram. Recording a value costs several
microseconds, and the consumer was recording three per message — enough that,
above roughly 10k messages/s, the harness fell behind and its own backlog
appeared in the results as broker latency.

So the consumer no longer builds histograms. It appends three integers to
preallocated arrays, which costs a fraction of a microsecond, and the
histograms are constructed after the run is over, where their cost cannot
affect anything.

The arrays are allocated once, up front. Growing them mid-run would put a
reallocation and copy inside the consumer loop, which is exactly the kind of
periodic stall that shows up as a fictitious tail.
"""

from __future__ import annotations

from array import array

from kpbench.metrics.histogram import LatencyRecorder

# Headroom above the expected message count, so that duplicate deliveries or a
# slightly over-running producer do not overflow the buffer.
CAPACITY_SLACK = 1.05


class SampleBuffer:
    """Fixed-capacity store of (recv, intended, send) timestamps in ns."""

    __slots__ = ("_capacity", "_intended", "_recv", "_send", "n", "overflow")

    def __init__(self, expected_samples: int) -> None:
        self._capacity = max(1024, int(expected_samples * CAPACITY_SLACK))
        # bytes() initialiser allocates the whole buffer in one go; appending
        # would grow it incrementally and copy during the run.
        self._recv = array("q", bytes(8 * self._capacity))
        self._intended = array("q", bytes(8 * self._capacity))
        self._send = array("q", bytes(8 * self._capacity))
        self.n = 0
        self.overflow = 0

    def add(self, recv_ns: int, intended_ns: int, send_ns: int) -> None:
        i = self.n
        if i >= self._capacity:
            # Counted rather than raising: losing the run at the last moment
            # would be worse than reporting a truncated one and saying so.
            self.overflow += 1
            return
        self._recv[i] = recv_ns
        self._intended[i] = intended_ns
        self._send[i] = send_ns
        self.n = i + 1

    def to_recorder(self) -> LatencyRecorder:
        """Build histograms from the captured samples. Run after measurement."""
        rec = LatencyRecorder()
        recv, intended, send = self._recv, self._intended, self._send
        for i in range(self.n):
            rec.record(recv[i], intended[i], send[i])
        return rec

    def throughput_per_second(self, origin_ns: int) -> dict[int, int]:
        """Messages received per whole second since ``origin_ns``."""
        out: dict[int, int] = {}
        recv = self._recv
        for i in range(self.n):
            bucket = (recv[i] - origin_ns) // 1_000_000_000
            out[bucket] = out.get(bucket, 0) + 1
        return out
