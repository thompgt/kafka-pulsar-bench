"""Tests for the open-loop scheduler.

The important one is `test_stall_widens_the_distribution`. It is the executable
form of invariant 1: it builds a deliberately stalling sink and asserts that
the stall shows up in the measured latency. A closed-loop generator passes
every other test in this file and fails that one, which is the entire point.
"""

from __future__ import annotations

import time

import pytest

from kpbench.metrics.histogram import LatencyRecorder, summarise
from kpbench.workload.scheduler import OpenLoopSchedule, intended_times, sleep_until


class TestIntendedTimes:
    def test_spacing_matches_rate(self) -> None:
        times = list(intended_times(rate_hz=1000, count=5, start_ns=0))
        assert times == [0, 1_000_000, 2_000_000, 3_000_000, 4_000_000]

    def test_no_drift_over_a_long_run(self) -> None:
        # Computed from the start rather than accumulated: repeatedly adding
        # an interval would drift, shifting every latency sample.
        rate = 7919.0  # prime, so the interval is not a round number
        count = 1_000_000
        times = list(intended_times(rate_hz=rate, count=count, start_ns=0))
        expected_last = int((count - 1) * (1_000_000_000.0 / rate))
        assert times[-1] == expected_last

    def test_rejects_non_positive_rate(self) -> None:
        with pytest.raises(ValueError, match="positive"):
            list(intended_times(rate_hz=0, count=1, start_ns=0))


class TestSleepUntil:
    def test_returns_immediately_when_target_has_passed(self) -> None:
        start = time.perf_counter_ns()
        sleep_until(start - 1_000_000)
        assert time.perf_counter_ns() - start < 5_000_000

    def test_does_not_return_early(self) -> None:
        # Overshoot is tolerable; undershoot corrupts the send schedule.
        target = time.perf_counter_ns() + 20_000_000
        sleep_until(target)
        assert time.perf_counter_ns() >= target


class TestOpenLoopSchedule:
    def test_message_counts_split_warmup_and_measured(self) -> None:
        s = OpenLoopSchedule(rate_hz=1000, duration_s=10, warmup_s=2, start_ns=0)
        assert s.warmup_messages == 2000
        assert s.total_messages == 12000
        assert s.measured_messages == 10000

    def test_warmup_classification(self) -> None:
        s = OpenLoopSchedule(rate_hz=100, duration_s=1, warmup_s=1, start_ns=0)
        assert s.is_warmup(0)
        assert s.is_warmup(99)
        assert not s.is_warmup(100)

    def test_pace_does_not_skip_late_messages(self) -> None:
        # Dropping late messages would hide overload, which is the failure mode
        # the benchmark exists to detect.
        s = OpenLoopSchedule(rate_hz=10_000, duration_s=0.05, warmup_s=0)
        seqs = [seq for seq, _ in s.pace()]
        assert seqs == list(range(s.total_messages))

    def test_rejects_bad_durations(self) -> None:
        with pytest.raises(ValueError, match="duration_s"):
            OpenLoopSchedule(rate_hz=100, duration_s=0)
        with pytest.raises(ValueError, match="warmup_s"):
            OpenLoopSchedule(rate_hz=100, duration_s=1, warmup_s=-1)


class TestCoordinatedOmission:
    """The property that makes this benchmark trustworthy."""

    STALL_MS = 200
    RATE_HZ = 1000.0
    DURATION_S = 0.6

    def _run_with_stall(self, *, closed_loop: bool) -> int:
        """Simulate a sink that freezes mid-run; return measured p99 in us.

        With ``closed_loop=True`` the schedule is re-anchored to actual send
        time after the stall, which is what a naive generator does. That is the
        bug being demonstrated, not an alternative mode the harness supports.
        """
        schedule = OpenLoopSchedule(
            rate_hz=self.RATE_HZ, duration_s=self.DURATION_S, warmup_s=0
        )
        recorder = LatencyRecorder()
        stall_at = schedule.total_messages // 2
        stalled = False
        offset_ns = 0

        for seq, intended_ns in schedule:
            target = intended_ns + offset_ns
            sleep_until(target)

            if seq == stall_at and not stalled:
                stalled = True
                time.sleep(self.STALL_MS / 1000.0)
                if closed_loop:
                    # The generator "catches its breath" and pretends the
                    # schedule restarted here. The stall disappears.
                    offset_ns += self.STALL_MS * 1_000_000

            send_ns = time.perf_counter_ns()
            recv_ns = send_ns  # instantaneous sink; isolates scheduling effects
            recorder.record(recv_ns, intended_ns + offset_ns, send_ns)

        return int(summarise(recorder.response).percentiles_us["p99"])

    def test_stall_widens_the_distribution(self) -> None:
        open_p99 = self._run_with_stall(closed_loop=False)
        # An open loop keeps the schedule running through the stall, so the
        # messages queued behind it are measurably late.
        assert open_p99 > self.STALL_MS * 1000 * 0.5, (
            f"open-loop p99 was {open_p99}us; a {self.STALL_MS}ms stall should "
            "be visible in the tail"
        )

    def test_closed_loop_hides_the_stall(self) -> None:
        closed_p99 = self._run_with_stall(closed_loop=True)
        open_p99 = self._run_with_stall(closed_loop=False)
        # Documents the failure mode rather than permitting it: the closed-loop
        # variant reports a far better tail for identical broker behaviour.
        assert closed_p99 < open_p99 / 2, (
            f"expected the closed-loop measurement to hide the stall "
            f"(closed p99={closed_p99}us vs open p99={open_p99}us)"
        )
