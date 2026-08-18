"""Tests for latency recording and histogram merging (invariant 2)."""

from __future__ import annotations

import pytest

from kpbench.metrics.histogram import (
    LatencyRecorder,
    decode_histogram,
    new_histogram,
    summarise,
)


class TestMerging:
    def test_merged_percentile_is_not_the_average_of_percentiles(self) -> None:
        # The reason invariant 2 exists. One fast population, one slow one:
        # averaging their p99s gives an answer nowhere near the true combined
        # p99, and this test pins the difference down.
        # 98/2 rather than 99/1: with exactly 1% slow, the p99 lands on the
        # boundary between the two populations and the test proves nothing.
        fast = new_histogram()
        slow = new_histogram()
        for _ in range(9800):
            fast.record_value(1000)  # 1ms
        for _ in range(200):
            slow.record_value(500_000)  # 500ms

        fast_p99 = fast.get_value_at_percentile(99.0)
        slow_p99 = slow.get_value_at_percentile(99.0)
        naive_average = (fast_p99 + slow_p99) / 2

        merged = new_histogram()
        merged.add(fast)
        merged.add(slow)
        true_p99 = merged.get_value_at_percentile(99.0)

        assert merged.get_total_count() == 10_000
        # The true p99 sits inside the slow tail; the naive average of the two
        # p99 values is dragged to roughly half of it.
        assert true_p99 >= 400_000
        assert naive_average < true_p99 / 1.5

    def test_recorder_merge_sums_counts(self) -> None:
        a, b = LatencyRecorder(), LatencyRecorder()
        for i in range(100):
            a.record(recv_ns=2_000_000 + i, intended_ns=0, send_ns=1_000_000)
        for i in range(50):
            b.record(recv_ns=5_000_000 + i, intended_ns=0, send_ns=1_000_000)
        a.merge(b)
        assert a.count == 150
        assert a.response.get_total_count() == 150


class TestRecording:
    def test_records_the_three_timings_separately(self) -> None:
        r = LatencyRecorder()
        # Intended at 0, actually sent 3ms late, received 10ms after intended.
        r.record(recv_ns=10_000_000, intended_ns=0, send_ns=3_000_000)
        # HdrHistogram reports the top of the containing bucket, so values are
        # exact only to the configured three significant figures.
        assert r.response.get_max_value() == pytest.approx(10_000, rel=1e-3)
        assert r.service.get_max_value() == pytest.approx(7_000, rel=1e-3)
        assert r.send_delay.get_max_value() == pytest.approx(3_000, rel=1e-3)

    def test_response_is_never_better_than_service(self) -> None:
        # Response time includes generator lag by construction. If it ever came
        # out lower, latency would be measured from the wrong origin.
        r = LatencyRecorder()
        for lag_ms in (0, 1, 5, 50):
            r.record(
                recv_ns=100_000_000,
                intended_ns=0,
                send_ns=lag_ms * 1_000_000,
            )
        assert r.response.get_mean_value() >= r.service.get_mean_value()

    def test_negative_interval_clamps_rather_than_drops(self) -> None:
        # Sub-microsecond clock jitter must not silently remove samples, which
        # would bias the distribution toward whatever remains.
        r = LatencyRecorder()
        r.record(recv_ns=0, intended_ns=500, send_ns=500)
        assert r.count == 1
        assert r.response.get_total_count() == 1


class TestSerialisation:
    def test_round_trip_preserves_the_distribution(self) -> None:
        # Manifests carry the encoded histogram so later analysis can merge
        # runs and recompute percentiles, instead of being stuck with the
        # percentiles chosen at run time.
        hist = new_histogram()
        for v in (10, 100, 1_000, 10_000, 100_000):
            for _ in range(100):
                hist.record_value(v)

        restored = decode_histogram(summarise(hist).encoded)
        assert restored.get_total_count() == hist.get_total_count()
        for p in (50.0, 99.0, 99.9):
            assert restored.get_value_at_percentile(p) == hist.get_value_at_percentile(p)

    def test_summary_of_empty_histogram_is_zeroed(self) -> None:
        s = summarise(new_histogram())
        assert s.count == 0
        assert s.percentiles_us["p99"] == 0
