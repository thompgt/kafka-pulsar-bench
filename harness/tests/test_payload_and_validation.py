"""Tests for payload encoding, keying, and delivery tracking."""

from __future__ import annotations

import pytest

from kpbench.workload.payload import HEADER_SIZE, PayloadCodec, key_for
from kpbench.workload.validation import DeliveryTracker


class TestPayloadCodec:
    def test_round_trip(self) -> None:
        codec = PayloadCodec(message_bytes=256)
        buf = codec.encode(seq=42, intended_ns=111, send_ns=222)
        assert len(buf) == 256
        t = codec.decode(buf)
        assert (t.seq, t.intended_ns, t.send_ns) == (42, 111, 222)

    def test_exact_message_size_at_the_minimum(self) -> None:
        codec = PayloadCodec(message_bytes=HEADER_SIZE)
        assert len(codec.encode(1, 2, 3)) == HEADER_SIZE

    def test_rejects_undersized_messages(self) -> None:
        with pytest.raises(ValueError, match=str(HEADER_SIZE)):
            PayloadCodec(message_bytes=HEADER_SIZE - 1)

    def test_rejects_foreign_payloads(self) -> None:
        # A dirty topic must be detected, not silently decoded into nonsense
        # sequence numbers that would corrupt the delivery stats.
        with pytest.raises(ValueError, match="magic"):
            PayloadCodec.decode(b"x" * 64)

    def test_rejects_truncated_payloads(self) -> None:
        with pytest.raises(ValueError, match="too short"):
            PayloadCodec.decode(b"KPB1")

    def test_random_fill_is_deterministic_for_a_seed(self) -> None:
        a = PayloadCodec(message_bytes=512, fill="random", seed=7)
        b = PayloadCodec(message_bytes=512, fill="random", seed=7)
        assert a.encode(0, 0, 0) == b.encode(0, 0, 0)

    def test_random_fill_differs_from_zero_fill(self) -> None:
        # Zero-filled payloads compress to nothing, which would make a
        # compression sweep meaningless.
        rnd = PayloadCodec(message_bytes=512, fill="random", seed=1).encode(0, 0, 0)
        zero = PayloadCodec(message_bytes=512, fill="zero").encode(0, 0, 0)
        assert rnd[HEADER_SIZE:] != zero[HEADER_SIZE:]
        assert zero[HEADER_SIZE:] == bytes(512 - HEADER_SIZE)


class TestKeying:
    def test_zero_cardinality_means_no_key(self) -> None:
        assert key_for(5, 0) is None

    def test_keys_spread_deterministically(self) -> None:
        keys = {key_for(i, 4) for i in range(100)}
        assert len(keys) == 4
        assert key_for(7, 4) == key_for(11, 4)


class TestDeliveryTracker:
    def test_counts_a_clean_run(self) -> None:
        t = DeliveryTracker(100)
        for i in range(100):
            t.observe(i)
        stats = t.finish(sent=100)
        assert (stats.missing, stats.duplicates, stats.out_of_order) == (0, 0, 0)
        assert stats.unique_received == 100

    def test_detects_gaps(self) -> None:
        t = DeliveryTracker(10)
        for i in (0, 1, 2, 4, 5, 6, 7, 8, 9):
            t.observe(i)
        stats = t.finish(sent=10)
        assert stats.missing == 1
        assert stats.missing_ratio == pytest.approx(0.1)

    def test_detects_duplicates_without_inflating_unique(self) -> None:
        t = DeliveryTracker(10)
        for i in range(10):
            t.observe(i)
        t.observe(3)
        stats = t.finish(sent=10)
        assert stats.duplicates == 1
        assert stats.received == 11
        assert stats.unique_received == 10
        assert stats.missing == 0

    def test_counts_reordering(self) -> None:
        # Expected with multiple partitions, so counted rather than treated as
        # an error.
        t = DeliveryTracker(5)
        for i in (0, 2, 1, 3, 4):
            t.observe(i)
        assert t.finish(sent=5).out_of_order == 1

    def test_rejects_out_of_range_sequences(self) -> None:
        t = DeliveryTracker(5)
        assert t.observe(5) is False
        assert t.observe(-1) is False
        assert t.finish(sent=5).received == 0
