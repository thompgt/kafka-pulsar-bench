"""Delivery validation (FR-4).

Latency numbers mean nothing without knowing whether the messages arrived. A
broker configured for weak durability can look wonderfully fast while quietly
dropping a percent of the load, so gaps, duplicates and reordering are tracked
for every run and the validity gate consults them.

Reordering is counted, not treated as an error: with more than one partition,
out-of-order arrival across partitions is expected behaviour rather than a
fault. It is recorded so that the degree of it is visible.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class DeliveryStats:
    sent: int
    received: int
    unique_received: int
    missing: int
    duplicates: int
    out_of_order: int
    late_arrivals_after_drain: int

    @property
    def missing_ratio(self) -> float:
        return self.missing / self.sent if self.sent else 0.0

    @property
    def duplicate_ratio(self) -> float:
        return self.duplicates / self.sent if self.sent else 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "sent": self.sent,
            "received": self.received,
            "unique_received": self.unique_received,
            "missing": self.missing,
            "duplicates": self.duplicates,
            "out_of_order": self.out_of_order,
            "late_arrivals_after_drain": self.late_arrivals_after_drain,
            "missing_ratio": self.missing_ratio,
            "duplicate_ratio": self.duplicate_ratio,
        }


class DeliveryTracker:
    """Tracks which sequence numbers arrived.

    A bytearray rather than a set: at a few million messages the set costs tens
    of megabytes and its allocation churn shows up as jitter in the consumer
    thread, which is the one place added latency would be misattributed to the
    broker.
    """

    __slots__ = ("_max_seq", "_seen", "_total", "duplicates", "out_of_order", "received")

    def __init__(self, total_messages: int) -> None:
        self._seen = bytearray(total_messages)
        self._total = total_messages
        self.received = 0
        self.duplicates = 0
        self.out_of_order = 0
        self._max_seq = -1

    def observe(self, seq: int) -> bool:
        """Record an arrival. Returns False if the sequence is out of range."""
        if seq < 0 or seq >= self._total:
            return False
        self.received += 1
        if self._seen[seq]:
            self.duplicates += 1
        else:
            self._seen[seq] = 1
        if seq < self._max_seq:
            self.out_of_order += 1
        else:
            self._max_seq = seq
        return True

    def unique_count(self) -> int:
        return sum(self._seen)

    def missing_count(self, sent: int) -> int:
        return sent - sum(self._seen[:sent])

    def finish(self, sent: int, late_arrivals: int = 0) -> DeliveryStats:
        unique = self.unique_count()
        return DeliveryStats(
            sent=sent,
            received=self.received,
            unique_received=unique,
            missing=self.missing_count(sent),
            duplicates=self.duplicates,
            out_of_order=self.out_of_order,
            late_arrivals_after_drain=late_arrivals,
        )
