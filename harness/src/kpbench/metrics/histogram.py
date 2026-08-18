"""Latency histograms (invariant 2).

Percentiles cannot be averaged. If two threads each report a p99, there is no
arithmetic that turns those two numbers into the p99 of the combined
population — you need the underlying distributions. So every latency sample
goes into an HdrHistogram, histograms are merged, and percentiles are computed
once from the merged result.

Samples are recorded in microseconds. Nanoseconds would push the dynamic range
far enough that the histogram's memory cost stops being free for no gain: sub
microsecond precision is meaningless across a network hop.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from hdrh.histogram import HdrHistogram

# 1 us to 5 minutes, three significant figures. The ceiling is deliberately far
# above any plausible latency: HdrHistogram silently refuses values above its
# maximum, and a dropped outlier is exactly the sample that mattered most.
LOWEST_US = 1
HIGHEST_US = 300_000_000
SIGNIFICANT_FIGURES = 3

REPORTED_PERCENTILES = (50.0, 75.0, 90.0, 95.0, 99.0, 99.9, 99.99)


def new_histogram() -> HdrHistogram:
    return HdrHistogram(LOWEST_US, HIGHEST_US, SIGNIFICANT_FIGURES)


class LatencyRecorder:
    """Records the three timings that together make omission visible.

    - ``response``: recv - intended. The headline number. Includes any time the
      generator itself was late, which is the point (invariant 1).
    - ``service``: recv - actual send. What a closed-loop benchmark would have
      reported. Kept so the difference between the two can be shown rather than
      asserted.
    - ``send_delay``: actual send - intended. Pure generator lag. If this is
      not near zero, the harness was struggling and the run is suspect.
    """

    __slots__ = ("response", "service", "send_delay", "count")

    def __init__(self) -> None:
        self.response = new_histogram()
        self.service = new_histogram()
        self.send_delay = new_histogram()
        self.count = 0

    def record(self, recv_ns: int, intended_ns: int, send_ns: int) -> None:
        # Clamp at zero rather than dropping: a negative value means clock
        # jitter of a few hundred nanoseconds, not a real event, and dropping
        # samples would bias the distribution.
        self.response.record_value(max(0, (recv_ns - intended_ns) // 1000))
        self.service.record_value(max(0, (recv_ns - send_ns) // 1000))
        self.send_delay.record_value(max(0, (send_ns - intended_ns) // 1000))
        self.count += 1

    def merge(self, other: LatencyRecorder) -> None:
        self.response.add(other.response)
        self.service.add(other.service)
        self.send_delay.add(other.send_delay)
        self.count += other.count


@dataclass(frozen=True)
class HistogramSummary:
    count: int
    min_us: int
    max_us: int
    mean_us: float
    percentiles_us: dict[str, int]
    encoded: str
    """Base64 HdrHistogram payload. Carried in the manifest so that later
    analysis can merge runs and recompute percentiles correctly, rather than
    being stuck with whatever percentiles were chosen today."""

    def to_dict(self) -> dict[str, Any]:
        return {
            "count": self.count,
            "min_us": self.min_us,
            "max_us": self.max_us,
            "mean_us": self.mean_us,
            "percentiles_us": self.percentiles_us,
            "encoded": self.encoded,
        }


def summarise(hist: HdrHistogram) -> HistogramSummary:
    total = hist.get_total_count()
    return HistogramSummary(
        count=total,
        min_us=hist.get_min_value() if total else 0,
        max_us=hist.get_max_value() if total else 0,
        mean_us=hist.get_mean_value() if total else 0.0,
        percentiles_us={
            f"p{p:g}": hist.get_value_at_percentile(p) if total else 0
            for p in REPORTED_PERCENTILES
        },
        encoded=hist.encode().decode("ascii"),
    )


def decode_histogram(encoded: str) -> HdrHistogram:
    """Rebuild a histogram from a manifest, for cross-run merging."""
    hist = new_histogram()
    hist.decode_and_add(encoded.encode("ascii"))
    return hist
