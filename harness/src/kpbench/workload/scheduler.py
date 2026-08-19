"""Open-loop rate scheduling (invariant 1).

The whole credibility of this project rests on this module, so it is worth
being explicit about what it does and why.

A *closed-loop* generator sends the next message once the previous one
completes. When the broker stalls for 200 ms, a closed-loop generator simply
sends nothing during the stall — and then reports the excellent latency of the
messages it did manage to send. The stall vanishes from the distribution. This
is coordinated omission, and it is why so many published benchmarks show
implausibly good tail latency.

An *open-loop* generator decides in advance when every message should be sent.
If the broker stalls, the schedule keeps advancing; the messages that could not
be sent on time accumulate lateness, and that lateness is measured. A 200 ms
stall widens the tail by roughly 200 ms, which is the honest answer.

So latency here is always measured from ``intended_ns`` — the schedule's
timestamp — never from when the send call actually happened.
"""

from __future__ import annotations

import time
from collections.abc import Iterator

# Below this, `time.sleep` cannot be trusted to return promptly enough, so the
# scheduler busy-waits instead. Spinning burns a core, which is why the window
# is kept small; sleeping through it would smear the send schedule.
#
# This is only an upper bound. The effective window is capped at half the send
# interval by `OpenLoopSchedule`, because a spin window wider than the interval
# means spinning continuously — and a Python busy-loop holds the GIL for a full
# `sys.getswitchinterval()` between yields, which starves the consumer thread
# and shows up as milliseconds of latency that no broker caused. That mistake
# put a 9ms p50 on an in-process loopback run before it was found.
_MAX_SPIN_NS = 300_000  # 300 us
_MIN_SPIN_NS = 2_000  # 2 us


def spin_window_for(rate_hz: float) -> int:
    """Spin window that leaves room for other threads to run."""
    interval_ns = 1_000_000_000.0 / rate_hz
    return max(_MIN_SPIN_NS, min(_MAX_SPIN_NS, int(interval_ns // 2)))


def intended_times(rate_hz: float, count: int, start_ns: int) -> Iterator[int]:
    """Yield the intended send time of each message.

    Computed from the start time rather than accumulated per message: adding an
    interval repeatedly lets floating-point error drift the schedule over a
    long run, which would silently shift every latency measurement.
    """
    if rate_hz <= 0:
        raise ValueError(f"rate_hz must be positive, got {rate_hz}")
    interval_ns = 1_000_000_000.0 / rate_hz
    for i in range(count):
        yield start_ns + int(i * interval_ns)


def sleep_until(target_ns: int, spin_ns: int = _MAX_SPIN_NS) -> None:
    """Block until ``target_ns`` on the perf counter, as precisely as possible.

    Coarse sleeping and then spinning the last fraction is the standard
    approach: `time.sleep` alone overshoots by enough to distort the send
    schedule, and spinning the whole interval would consume a core the broker
    needs — and, in Python, would hold the GIL against the consumer thread.
    """
    while True:
        remaining = target_ns - time.perf_counter_ns()
        if remaining <= 0:
            return
        if remaining > spin_ns:
            time.sleep((remaining - spin_ns) / 1e9)
        else:
            while time.perf_counter_ns() < target_ns:
                pass
            return


class OpenLoopSchedule:
    """The send schedule for one run, warm-up included.

    Warm-up messages are sent exactly like measured ones — the broker must be
    warm under real load — but their samples are discarded (requirement M-5).
    """

    def __init__(
        self,
        rate_hz: float,
        duration_s: float,
        warmup_s: float = 0.0,
        start_ns: int | None = None,
    ) -> None:
        if duration_s <= 0:
            raise ValueError(f"duration_s must be positive, got {duration_s}")
        if warmup_s < 0:
            raise ValueError(f"warmup_s must not be negative, got {warmup_s}")

        self.rate_hz = rate_hz
        self.duration_s = duration_s
        self.warmup_s = warmup_s
        self.warmup_messages = int(rate_hz * warmup_s)
        self.total_messages = int(rate_hz * (warmup_s + duration_s))
        self.measured_messages = self.total_messages - self.warmup_messages
        self.start_ns = time.perf_counter_ns() if start_ns is None else start_ns
        self.spin_ns = spin_window_for(rate_hz)

    @property
    def end_ns(self) -> int:
        return self.start_ns + int((self.warmup_s + self.duration_s) * 1e9)

    def is_warmup(self, seq: int) -> bool:
        return seq < self.warmup_messages

    def __iter__(self) -> Iterator[tuple[int, int]]:
        """Yield ``(seq, intended_ns)`` pairs. Does not sleep; the caller does."""
        yield from enumerate(
            intended_times(self.rate_hz, self.total_messages, self.start_ns)
        )

    def pace(self) -> Iterator[tuple[int, int]]:
        """Yield ``(seq, intended_ns)``, sleeping until each intended time.

        Deliberately does *not* skip messages that are already late. Falling
        behind is a measurable property of the run — it is what makes the
        generator's own lag visible — and dropping late messages would hide
        exactly the overload the benchmark is trying to observe.
        """
        for seq, intended_ns in self:
            sleep_until(intended_ns, self.spin_ns)
            yield seq, intended_ns
