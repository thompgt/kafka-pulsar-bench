"""Benchmark execution.

Phase order matters and is worth stating, because several of these steps exist
only to prevent a specific way of getting a plausible wrong answer:

1. Provision the topic and wait for metadata to settle — otherwise the first
   messages hit an unknown topic and record latency that belongs to setup.
2. Start the consumer and wait for it to be genuinely assigned — otherwise the
   first messages sit unread and record enormous latency that belongs to the
   harness.
3. Send warm-up traffic under real load, and discard its samples (M-5).
4. Send measured traffic on the open-loop schedule (invariant 1).
5. Flush, then drain until every message is accounted for or the drain times
   out — a run that stops polling early reports the latency of the messages
   that happened to be quick.
6. Decide validity *before* reporting anything (M-8).
"""

from __future__ import annotations

import platform
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from kpbench.config import RunConfig
from kpbench.drivers.base import Driver
from kpbench.metrics.histogram import summarise
from kpbench.metrics.samples import SampleBuffer
from kpbench.workload.payload import HEADER_STRUCT, MAGIC, PayloadCodec, key_for
from kpbench.workload.scheduler import OpenLoopSchedule
from kpbench.workload.validation import DeliveryTracker

# See the note where this is applied: the CPython default of 5ms lets the
# consumer thread wait milliseconds for the GIL, which is then indistinguishable
# from broker latency in the results.
SWITCH_INTERVAL_S = 0.0002

# Bound once: the consumer loop unpacks inline rather than calling a helper
# that would allocate a result object for every message.
_HEADER_UNPACK = HEADER_STRUCT.unpack_from


@dataclass
class RunOutcome:
    run_id: str
    valid: bool
    reasons: list[str] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)


class BenchmarkRunner:
    def __init__(self, config: RunConfig, driver: Driver) -> None:
        self.config = config
        self.driver = driver
        self.run_id = f"{config.name}-{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"

        w = config.workload
        self.schedule = OpenLoopSchedule(
            rate_hz=w.target_rate_hz,
            duration_s=w.duration_s,
            warmup_s=w.warmup_s,
            start_ns=0,  # replaced at send time; see run()
        )
        self.codec = PayloadCodec(
            message_bytes=w.message_bytes,
            fill=w.payload_fill.value,
            seed=config.seed,
        )
        self.samples = SampleBuffer(max(1, self.schedule.measured_messages))
        self.tracker = DeliveryTracker(max(1, self.schedule.measured_messages))

        self._stop = threading.Event()
        self._consumer_error: BaseException | None = None
        self._throughput: dict[int, int] = {}
        self._measure_start_ns = 0
        self._unknown_seq = 0

    # --- consumer thread -------------------------------------------------
    def _consume_loop(self) -> None:
        """Kept as lean as it can be made.

        Everything in this loop is paid per message while measurement is
        running, so anything that can be deferred to after the run is deferred:
        histogram construction, throughput bucketing, and percentile
        computation all happen later. Locals are bound up front to avoid
        attribute lookups, and the payload header is unpacked inline rather
        than through a helper that would allocate a result object per message.
        """
        warmup = self.schedule.warmup_messages
        unpack = _HEADER_UNPACK
        add_sample = self.samples.add
        observe = self.tracker.observe
        poll = self.driver.poll
        poll_timeout_s = self.config.consumer.poll_timeout_ms / 1000.0
        now = time.perf_counter_ns
        stopped = self._stop.is_set
        try:
            while not stopped():
                payloads = poll(poll_timeout_s)
                if not payloads:
                    continue
                recv_ns = now()
                for buf in payloads:
                    try:
                        magic, seq, intended_ns, send_ns = unpack(buf, 0)
                    except Exception:
                        self._unknown_seq += 1
                        continue
                    if magic != MAGIC:
                        # Foreign message on the topic. Counted, not fatal:
                        # silently ignoring it would hide a dirty topic.
                        self._unknown_seq += 1
                        continue
                    if seq < warmup:
                        continue  # warm-up sample, discarded (M-5)
                    add_sample(recv_ns, intended_ns, send_ns)
                    observe(seq - warmup)
        except BaseException as exc:
            self._consumer_error = exc

    # --- main ------------------------------------------------------------
    def run(self) -> RunOutcome:
        cfg = self.config
        started_at = datetime.now(UTC)

        self.driver.provision()
        self.driver.start_consumer()
        if not self.driver.wait_consumer_ready(timeout_s=60.0):
            raise RuntimeError(
                "consumer was not ready within 60s; producing now would record "
                "setup latency as broker latency"
            )
        self.driver.start_producer()

        # The producer and consumer threads contend for the GIL. At the default
        # 5ms switch interval the consumer can wait milliseconds to be
        # scheduled, and that wait is recorded as latency the broker never
        # caused. Tightening it trades a little throughput for a measurement
        # that reflects the system under test rather than CPython's scheduler.
        previous_switch_interval = sys.getswitchinterval()
        sys.setswitchinterval(SWITCH_INTERVAL_S)

        consumer_thread = threading.Thread(
            target=self._consume_loop, name="kpbench-consumer", daemon=True
        )
        consumer_thread.start()

        # The schedule is anchored here, after all setup, so that setup cost
        # never lands inside a measured interval.
        self.schedule.start_ns = time.perf_counter_ns()
        self._measure_start_ns = self.schedule.start_ns + int(cfg.workload.warmup_s * 1e9)

        sent = 0
        sent_measured = 0
        send_wall_start = time.perf_counter_ns()
        for seq, intended_ns in self.schedule.pace():
            send_ns = time.perf_counter_ns()
            payload = self.codec.encode(seq, intended_ns, send_ns)
            self.driver.send(key_for(seq, cfg.workload.key_cardinality), payload)
            sent += 1
            if seq >= self.schedule.warmup_messages:
                sent_measured += 1
        send_wall_end = time.perf_counter_ns()

        unsent = self.driver.flush(timeout_s=cfg.validity.drain_timeout_s)

        # Drain: keep consuming until everything sent has arrived, or the
        # timeout expires. Stopping at the first quiet moment would silently
        # exclude the slowest messages, which are the ones that matter.
        drain_deadline = time.monotonic() + cfg.validity.drain_timeout_s
        while time.monotonic() < drain_deadline:
            if self.tracker.unique_count() >= sent_measured:
                break
            time.sleep(0.05)

        self._stop.set()
        consumer_thread.join(timeout=10.0)
        sys.setswitchinterval(previous_switch_interval)
        if self._consumer_error is not None:
            raise self._consumer_error

        delivery = self.tracker.finish(sent=sent_measured)

        elapsed_s = (send_wall_end - send_wall_start) / 1e9
        achieved_rate = sent / elapsed_s if elapsed_s > 0 else 0.0
        achieved_ratio = achieved_rate / cfg.workload.target_rate_hz

        # Histograms are built here, after measurement has finished, so their
        # cost cannot appear in the results they describe.
        recorder = self.samples.to_recorder()
        self._throughput = self.samples.throughput_per_second(self._measure_start_ns)
        response = summarise(recorder.response)
        service = summarise(recorder.service)
        send_delay = summarise(recorder.send_delay)

        reasons: list[str] = []
        # Platform first: on Windows nothing else in this list can be trusted,
        # because the poll granularity exceeds the latencies being measured.
        if platform.system() == "Windows" and not cfg.validity.allow_windows_host:
            reasons.append(
                "executed on a Windows host, where poll waits quantise to the "
                "~15.6ms scheduler tick, larger than the latencies being "
                "measured. Run under WSL2 or Linux (ADR-0003)."
            )
        if achieved_ratio < cfg.validity.min_achieved_rate_ratio:
            reasons.append(
                f"generator sustained only {achieved_rate:,.0f}/s of the "
                f"{cfg.workload.target_rate_hz:,.0f}/s target "
                f"({achieved_ratio:.1%}); this measures the harness ceiling, not the broker"
            )
        if delivery.missing_ratio > cfg.validity.max_missing_ratio:
            reasons.append(
                f"{delivery.missing:,} of {sent_measured:,} messages never arrived "
                f"({delivery.missing_ratio:.3%})"
            )
        if delivery.duplicate_ratio > cfg.validity.max_duplicate_ratio:
            reasons.append(
                f"{delivery.duplicates:,} duplicate deliveries ({delivery.duplicate_ratio:.3%})"
            )
        if unsent:
            reasons.append(f"{unsent:,} messages were still unsent when flush timed out")
        if self.samples.overflow:
            reasons.append(
                f"{self.samples.overflow:,} samples exceeded the capture buffer; "
                "the reported distribution is truncated"
            )
        if self._unknown_seq:
            reasons.append(
                f"{self._unknown_seq:,} unrecognised messages on the topic; "
                "was it left dirty by an earlier run?"
            )

        metrics: dict[str, Any] = {
            "started_at": started_at.isoformat(),
            "finished_at": datetime.now(UTC).isoformat(),
            "elapsed_s": elapsed_s,
            "sent_total": sent,
            "sent_measured": sent_measured,
            "target_rate_hz": cfg.workload.target_rate_hz,
            "achieved_rate_hz": achieved_rate,
            "achieved_rate_ratio": achieved_ratio,
            "warmup_messages_discarded": self.schedule.warmup_messages,
            "warmup_s": cfg.workload.warmup_s,
            "delivery": delivery.to_dict(),
            "latency": {
                "response": response.to_dict(),
                "service": service.to_dict(),
                "generator_lag": send_delay.to_dict(),
            },
            "throughput_series": [
                {"second": s, "messages": n} for s, n in sorted(self._throughput.items())
            ],
        }

        return RunOutcome(
            run_id=self.run_id, valid=not reasons, reasons=reasons, metrics=metrics
        )

    def cleanup(self) -> None:
        try:
            self.driver.deprovision()
        finally:
            self.driver.close()
