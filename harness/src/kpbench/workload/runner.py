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

import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from kpbench.config import RunConfig
from kpbench.drivers.base import Driver
from kpbench.metrics.histogram import LatencyRecorder, summarise
from kpbench.workload.payload import PayloadCodec, key_for
from kpbench.workload.scheduler import OpenLoopSchedule
from kpbench.workload.validation import DeliveryTracker


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
        self.recorder = LatencyRecorder()
        self.tracker = DeliveryTracker(max(1, self.schedule.measured_messages))

        self._stop = threading.Event()
        self._consumer_error: BaseException | None = None
        self._throughput: dict[int, int] = {}
        self._measure_start_ns = 0
        self._unknown_seq = 0

    # --- consumer thread -------------------------------------------------
    def _consume_loop(self) -> None:
        warmup = self.schedule.warmup_messages
        try:
            while not self._stop.is_set():
                payloads = self.driver.poll(0.1)
                if not payloads:
                    continue
                recv_ns = time.perf_counter_ns()
                for buf in payloads:
                    try:
                        t = PayloadCodec.decode(buf)
                    except ValueError:
                        # Foreign message on the topic. Counted, not fatal:
                        # silently ignoring it would hide a dirty topic.
                        self._unknown_seq += 1
                        continue
                    if t.seq < warmup:
                        continue  # warm-up sample, discarded (M-5)
                    self.recorder.record(recv_ns, t.intended_ns, t.send_ns)
                    self.tracker.observe(t.seq - warmup)
                    bucket = (recv_ns - self._measure_start_ns) // 1_000_000_000
                    self._throughput[bucket] = self._throughput.get(bucket, 0) + 1
        except BaseException as exc:  # noqa: BLE001 - re-raised on the main thread
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
        if self._consumer_error is not None:
            raise self._consumer_error

        delivery = self.tracker.finish(sent=sent_measured)

        elapsed_s = (send_wall_end - send_wall_start) / 1e9
        achieved_rate = sent / elapsed_s if elapsed_s > 0 else 0.0
        achieved_ratio = achieved_rate / cfg.workload.target_rate_hz

        response = summarise(self.recorder.response)
        service = summarise(self.recorder.service)
        send_delay = summarise(self.recorder.send_delay)

        reasons: list[str] = []
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
