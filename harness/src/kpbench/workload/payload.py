"""Message payload encoding.

Every message carries the timing information the measurement depends on:

    magic | seq | intended_ns | send_ns

``intended_ns`` is the one that matters. It is when the open-loop schedule said
this message *should* have been sent, and end-to-end latency is measured from
it (invariant 1). ``send_ns`` is when the producer actually got to it; the gap
between the two is generator lag, which is reported separately so that a
harness that fell behind is visible rather than flattering.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass

MAGIC = b"KPB1"

# little-endian: magic(4) seq(8, unsigned) intended_ns(8, signed) send_ns(8, signed)
_HEADER = struct.Struct("<4sQqq")
HEADER_SIZE = _HEADER.size  # 28

# Exported so the consumer hot loop can unpack inline. Decoding through
# `PayloadCodec.decode` allocates a Timing object per message, which is real
# cost when it happens tens of thousands of times a second.
HEADER_STRUCT = _HEADER


@dataclass(frozen=True, slots=True)
class Timing:
    seq: int
    intended_ns: int
    send_ns: int


class PayloadCodec:
    """Builds fixed-size payloads with a timing header.

    The filler is generated once and sliced, rather than generated per message:
    per-message random generation would put a CPU cost inside the send loop and
    show up as harness latency.
    """

    def __init__(self, message_bytes: int, fill: str = "random", seed: int = 0) -> None:
        if message_bytes < HEADER_SIZE:
            raise ValueError(f"message_bytes must be >= {HEADER_SIZE}, got {message_bytes}")
        self.message_bytes = message_bytes
        self._filler_len = message_bytes - HEADER_SIZE

        if fill == "zero":
            self._filler = bytes(self._filler_len)
        else:
            # Deterministic given the seed, so a run is reproducible, but
            # incompressible enough that compression sweeps mean something.
            import random

            rng = random.Random(seed)
            self._filler = rng.randbytes(self._filler_len)

    def encode(self, seq: int, intended_ns: int, send_ns: int) -> bytes:
        return _HEADER.pack(MAGIC, seq, intended_ns, send_ns) + self._filler

    @staticmethod
    def decode(buf: bytes) -> Timing:
        if len(buf) < HEADER_SIZE:
            raise ValueError(f"payload too short: {len(buf)} < {HEADER_SIZE}")
        magic, seq, intended_ns, send_ns = _HEADER.unpack_from(buf, 0)
        if magic != MAGIC:
            raise ValueError(f"bad magic {magic!r}; not a kpbench payload")
        return Timing(seq=seq, intended_ns=intended_ns, send_ns=send_ns)


def key_for(seq: int, cardinality: int) -> bytes | None:
    """Partition key for a sequence number.

    Cardinality 0 means no key, which lets the broker distribute freely. Any
    other value spreads deterministically over that many distinct keys, so a
    run is reproducible and key skew is a property of the config rather than
    an accident.
    """
    if cardinality <= 0:
        return None
    return b"k%d" % (seq % cardinality)
