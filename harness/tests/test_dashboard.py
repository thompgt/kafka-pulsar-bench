"""Unit tests for kpbench monitoring metrics exporter and web dashboard."""

from __future__ import annotations

import json
import pathlib
import urllib.request

import pytest

from kpbench.dashboard.server import DashboardServer, get_run_details, list_runs
from kpbench.metrics.exporter import MetricsExporterServer, format_prometheus_metrics

# ---------------------------------------------------------------------------
# Metrics Exporter Tests
# ---------------------------------------------------------------------------


def test_format_prometheus_metrics_empty() -> None:
    text = format_prometheus_metrics({}, driver="test-driver")
    assert 'kpbench_target_rate_hz{driver="test-driver"} 0.0' in text
    assert 'kpbench_achieved_rate_hz{driver="test-driver"} 0.0' in text
    assert 'kpbench_messages_sent_total{driver="test-driver"} 0' in text
    assert 'kpbench_messages_received_total{driver="test-driver"} 0' in text
    assert 'kpbench_messages_missing_total{driver="test-driver"} 0' in text


def test_format_prometheus_metrics_populated() -> None:
    sample_metrics = {
        "target_rate_hz": 10000.0,
        "achieved_rate_hz": 9950.5,
        "achieved_rate_ratio": 0.99505,
        "sent_total": 50000,
        "delivery": {
            "received": 50000,
            "missing": 0,
            "duplicates": 0,
        },
        "latency": {
            "response": {
                "percentiles_us": {
                    "p50": 120.0,
                    "p99": 450.0,
                    "p99.9": 1100.0,
                },
                "max_us": 2500.0,
            },
            "service": {
                "percentiles_us": {
                    "p50": 80.0,
                    "p99": 300.0,
                },
                "max_us": 1200.0,
            },
        },
    }

    text = format_prometheus_metrics(sample_metrics, driver="kafka")
    assert 'kpbench_target_rate_hz{driver="kafka"} 10000.0' in text
    assert 'kpbench_achieved_rate_hz{driver="kafka"} 9950.5' in text
    assert 'kpbench_achieved_rate_ratio{driver="kafka"} 0.99505' in text
    assert 'kpbench_messages_sent_total{driver="kafka"} 50000' in text
    assert 'kpbench_messages_received_total{driver="kafka"} 50000' in text
    assert 'kpbench_messages_missing_total{driver="kafka"} 0' in text
    assert (
        'kpbench_latency_microseconds{driver="kafka",series="response",percentile="p50"} 120.0'
        in text
    )
    assert (
        'kpbench_latency_microseconds{driver="kafka",series="response",percentile="p99"} 450.0'
        in text
    )
    assert (
        'kpbench_latency_microseconds{driver="kafka",series="response",percentile="p99.9"} 1100.0'
        in text
    )
    assert (
        'kpbench_latency_microseconds{driver="kafka",series="response",percentile="max"} 2500.0'
        in text
    )
    assert (
        'kpbench_latency_microseconds{driver="kafka",series="service",percentile="p50"} 80.0'
        in text
    )


def test_metrics_exporter_server() -> None:
    server = MetricsExporterServer(host="127.0.0.1", port=0)
    server.start()
    try:
        base_url = f"http://127.0.0.1:{server.port}"

        # Test /health
        with urllib.request.urlopen(f"{base_url}/health", timeout=3.0) as resp:
            assert resp.status == 200
            assert resp.read() == b'{"status":"healthy"}'

        # Update metrics dynamically
        server.update_metrics(
            {"target_rate_hz": 5000.0, "achieved_rate_hz": 4990.0},
            driver_name="pulsar",
        )

        # Test /metrics
        with urllib.request.urlopen(f"{base_url}/metrics", timeout=3.0) as resp:
            assert resp.status == 200
            body = resp.read().decode("utf-8")
            assert 'kpbench_target_rate_hz{driver="pulsar"} 5000.0' in body
            assert 'kpbench_achieved_rate_hz{driver="pulsar"} 4990.0' in body

        # Test 404
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            urllib.request.urlopen(f"{base_url}/nonexistent", timeout=3.0)
        assert exc_info.value.code == 404
    finally:
        server.stop()


# ---------------------------------------------------------------------------
# Dashboard Manifest & Server Tests
# ---------------------------------------------------------------------------


def test_list_runs_nonexistent(tmp_path: pathlib.Path) -> None:
    assert list_runs(tmp_path / "does_not_exist") == []


def test_list_runs_and_get_details(tmp_path: pathlib.Path) -> None:
    runs_dir = tmp_path / "results"
    runs_dir.mkdir()

    # Valid run 1
    run1 = runs_dir / "run-001"
    run1.mkdir()
    manifest1 = {
        "manifest_version": "1.0.0",
        "run_id": "run-001",
        "valid": True,
        "invalid_reasons": [],
        "warmup_run": False,
        "driver": "kafka",
        "config": {
            "workload": {"message_bytes": 256},
            "topic": {"partitions": 3},
        },
        "metrics": {
            "target_rate_hz": 20000,
            "achieved_rate_hz": 19980,
            "achieved_rate_ratio": 0.999,
            "started_at": "2026-09-07T12:00:00Z",
            "latency": {
                "response": {
                    "percentiles_us": {
                        "p50": 150.0,
                        "p99": 620.0,
                        "p99.9": 1400.0,
                    }
                }
            },
        },
    }
    (run1 / "manifest.json").write_text(json.dumps(manifest1), encoding="utf-8")

    # Corrupt run (invalid JSON)
    run2 = runs_dir / "run-corrupt"
    run2.mkdir()
    (run2 / "manifest.json").write_text("invalid json content", encoding="utf-8")

    # Dir without manifest
    run3 = runs_dir / "run-empty"
    run3.mkdir()

    # Plain file in results_dir (should be ignored)
    (runs_dir / "notes.txt").write_text("hello", encoding="utf-8")

    runs = list_runs(runs_dir)
    assert len(runs) == 1
    assert runs[0]["run_id"] == "run-001"
    assert runs[0]["driver"] == "kafka"
    assert runs[0]["valid"] is True
    assert runs[0]["achieved_rate_hz"] == 19980
    assert runs[0]["p50_us"] == 150.0
    assert runs[0]["p99_us"] == 620.0
    assert runs[0]["p99_9_us"] == 1400.0
    assert runs[0]["message_bytes"] == 256
    assert runs[0]["partitions"] == 3

    # Details
    details = get_run_details(runs_dir, "run-001")
    assert details is not None
    assert details["run_id"] == "run-001"

    # Nonexistent details
    assert get_run_details(runs_dir, "run-999") is None

    # Corrupted details
    assert get_run_details(runs_dir, "run-corrupt") is None


def test_dashboard_server(tmp_path: pathlib.Path) -> None:
    runs_dir = tmp_path / "results"
    runs_dir.mkdir()

    run_dir = runs_dir / "run-test"
    run_dir.mkdir()
    manifest_data = {
        "manifest_version": "1.0.0",
        "run_id": "run-test",
        "valid": True,
        "driver": "pulsar",
        "metrics": {"achieved_rate_hz": 12500},
    }
    (run_dir / "manifest.json").write_text(json.dumps(manifest_data), encoding="utf-8")

    server = DashboardServer(host="127.0.0.1", port=0, results_dir=runs_dir)
    server.start()
    try:
        base_url = f"http://127.0.0.1:{server.port}"

        # 1. /api/health
        with urllib.request.urlopen(f"{base_url}/api/health", timeout=3.0) as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode("utf-8"))
            assert data["status"] == "healthy"

        # 2. /api/runs
        with urllib.request.urlopen(f"{base_url}/api/runs", timeout=3.0) as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode("utf-8"))
            assert len(data) == 1
            assert data[0]["run_id"] == "run-test"

        # 3. /api/runs/<run_id>
        with urllib.request.urlopen(f"{base_url}/api/runs/run-test", timeout=3.0) as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode("utf-8"))
            assert data["run_id"] == "run-test"
            assert data["driver"] == "pulsar"

        # 4. /api/runs/<missing>
        with pytest.raises(urllib.error.HTTPError) as exc:
            urllib.request.urlopen(f"{base_url}/api/runs/run-missing", timeout=3.0)
        assert exc.value.code == 404

        # 5. Static assets: / (index.html)
        with urllib.request.urlopen(f"{base_url}/", timeout=3.0) as resp:
            assert resp.status == 200
            content = resp.read().decode("utf-8")
            assert "<!DOCTYPE html>" in content
            assert "kpbench" in content

        # 6. Static assets: style.css
        with urllib.request.urlopen(f"{base_url}/style.css", timeout=3.0) as resp:
            assert resp.status == 200
            css = resp.read().decode("utf-8")
            assert "--bg-base" in css

        # 7. Static assets: app.js
        with urllib.request.urlopen(f"{base_url}/app.js", timeout=3.0) as resp:
            assert resp.status == 200
            js = resp.read().decode("utf-8")
            assert "fetchRuns" in js
    finally:
        server.stop()
