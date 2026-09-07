"""Prometheus & OpenTelemetry-compatible metrics exporter for kpbench.

Formats live benchmark metrics into Prometheus exposition text format so that
Prometheus, OpenTelemetry Collectors, and Grafana can scrape them directly
from the harness during or after runs.
"""

from __future__ import annotations

import http.server
import threading
from typing import Any


def format_prometheus_metrics(metrics: dict[str, Any], driver: str = "unknown") -> str:
    """Render kpbench metrics into Prometheus exposition text format."""
    lines: list[str] = [
        "# HELP kpbench_target_rate_hz Target rate configured for the benchmark",
        "# TYPE kpbench_target_rate_hz gauge",
        f'kpbench_target_rate_hz{{driver="{driver}"}} {metrics.get("target_rate_hz", 0.0)}',
        "",
        "# HELP kpbench_achieved_rate_hz Actual sustained send rate",
        "# TYPE kpbench_achieved_rate_hz gauge",
        f'kpbench_achieved_rate_hz{{driver="{driver}"}} {metrics.get("achieved_rate_hz", 0.0)}',
        "",
        "# HELP kpbench_achieved_rate_ratio Ratio of achieved rate to target rate",
        "# TYPE kpbench_achieved_rate_ratio gauge",
        (
            f'kpbench_achieved_rate_ratio{{driver="{driver}"}} '
            f'{metrics.get("achieved_rate_ratio", 0.0)}'
        ),
        "",
    ]

    delivery = metrics.get("delivery", {})
    lines.extend([
        "# HELP kpbench_messages_sent_total Total messages sent by producer",
        "# TYPE kpbench_messages_sent_total counter",
        f'kpbench_messages_sent_total{{driver="{driver}"}} {metrics.get("sent_total", 0)}',
        "",
        "# HELP kpbench_messages_received_total Messages received by consumer",
        "# TYPE kpbench_messages_received_total counter",
        f'kpbench_messages_received_total{{driver="{driver}"}} {delivery.get("received", 0)}',
        "",
        "# HELP kpbench_messages_missing_total Messages missing/lost",
        "# TYPE kpbench_messages_missing_total gauge",
        f'kpbench_messages_missing_total{{driver="{driver}"}} {delivery.get("missing", 0)}',
        "",
        "# HELP kpbench_messages_duplicate_total Duplicate deliveries observed",
        "# TYPE kpbench_messages_duplicate_total counter",
        f'kpbench_messages_duplicate_total{{driver="{driver}"}} {delivery.get("duplicates", 0)}',
        "",
    ])

    latency = metrics.get("latency", {})
    lines.extend([
        "# HELP kpbench_latency_microseconds Latency percentiles in microseconds",
        "# TYPE kpbench_latency_microseconds gauge",
    ])
    for series, data in latency.items():
        percentiles = data.get("percentiles_us", {})
        for p_name, val in percentiles.items():
            lbls = f'driver="{driver}",series="{series}",percentile="{p_name}"'
            lines.append(f"kpbench_latency_microseconds{{{lbls}}} {val}")
        max_val = data.get("max_us", 0)
        lbls = f'driver="{driver}",series="{series}",percentile="max"'
        lines.append(f"kpbench_latency_microseconds{{{lbls}}} {max_val}")

    lines.append("")
    return "\n".join(lines)


class _MetricsHandler(http.server.BaseHTTPRequestHandler):
    data_provider: Any = None

    def do_GET(self) -> None:
        if self.path in ("/metrics", "/"):
            data_fn = self.data_provider if callable(self.data_provider) else None
            metrics, driver = data_fn() if data_fn else ({}, "kpbench")
            body = format_prometheus_metrics(metrics, driver).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            body = b'{"status":"healthy"}'
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format: str, *args: Any) -> None:
        pass  # Quiet logs during benchmarking


class MetricsExporterServer:
    """Lightweight HTTP server serving Prometheus /metrics."""

    def __init__(self, host: str = "0.0.0.0", port: int = 9102) -> None:
        self.host = host
        self._current_metrics: dict[str, Any] = {}
        self._driver_name = "kpbench"

        handler_cls = type(
            "BoundMetricsHandler",
            (_MetricsHandler,),
            {
                "data_provider": staticmethod(lambda: (self._current_metrics, self._driver_name)),
            },
        )
        self.server = http.server.HTTPServer((self.host, port), handler_cls)
        self.port: int = self.server.server_address[1]
        self._thread: threading.Thread | None = None

    def update_metrics(self, metrics: dict[str, Any], driver_name: str = "kpbench") -> None:
        self._current_metrics = metrics
        self._driver_name = driver_name

    def start(self) -> None:
        self._thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
