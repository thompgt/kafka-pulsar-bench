"""Unit tests for Iceberg results warehouse integration."""

from __future__ import annotations

import json
import pathlib
from typing import Any

import pytest
from pyiceberg.catalog import load_in_memory

from kpbench.results.warehouse import IcebergWarehouse


def _sample_manifest(
    run_id: str = "run-sample-001",
    driver: str = "kafka",
    valid: bool = True,
) -> dict[str, Any]:
    return {
        "manifest_version": 1,
        "run_id": run_id,
        "driver": driver,
        "valid": valid,
        "invalid_reasons": [] if valid else ["rate starved"],
        "warmup_run": False,
        "config": {
            "name": "smoke",
            "workload": {"target_rate_hz": 10000.0, "duration_s": 5.0},
        },
        "environment": {"host": {"os": "Linux"}},
        "client": {"version": "2.5.0"},
        "metrics": {
            "started_at": "2026-09-07T12:00:00.123456+00:00",
            "target_rate_hz": 10000.0,
            "achieved_rate_hz": 9985.0,
            "achieved_rate_ratio": 0.9985,
            "sent_total": 50000,
            "sent_measured": 50000,
            "delivery": {
                "received": 50000,
                "missing": 0,
                "duplicates": 0,
                "out_of_order": 2,
            },
            "latency": {
                "response": {
                    "count": 50000,
                    "min_us": 100,
                    "max_us": 15000,
                    "mean_us": 450.0,
                    "percentiles_us": {
                        "p50": 320.0,
                        "p75": 480.0,
                        "p90": 720.0,
                        "p95": 950.0,
                        "p99": 1400.0,
                        "p99.9": 3500.0,
                        "p99.99": 8000.0,
                    },
                    "encoded": "HISTFAAACqZ4nE1YO5YkOxEdRUZF...",
                },
                "service": {
                    "count": 50000,
                    "min_us": 80,
                    "max_us": 8000,
                    "mean_us": 250.0,
                    "percentiles_us": {
                        "p50": 210.0,
                        "p75": 310.0,
                        "p90": 450.0,
                        "p95": 580.0,
                        "p99": 920.0,
                        "p99.9": 1800.0,
                        "p99.99": 4000.0,
                    },
                    "encoded": "HISTFAAACqZ4nE1YO5YkOxEdRUZF...",
                },
            },
            "throughput_series": [
                {"second": 1, "messages": 9990},
                {"second": 2, "messages": 9980},
                {"second": 3, "messages": 9985},
            ],
        },
    }


@pytest.fixture
def in_memory_warehouse(tmp_path: pathlib.Path) -> IcebergWarehouse:
    cat = load_in_memory("test_cat", {"warehouse": str(tmp_path / "warehouse")})
    return IcebergWarehouse(catalog=cat, namespace="bench")


def test_warehouse_table_creation(in_memory_warehouse: IcebergWarehouse) -> None:
    wh = in_memory_warehouse
    tables = [t[1] for t in wh.catalog.list_tables(wh.namespace)]
    assert "runs" in tables
    assert "latency_samples" in tables
    assert "throughput_series" in tables

    # Check partition spec on runs
    runs_tbl = wh.runs_table
    assert any(f.name == "driver" for f in runs_tbl.spec().fields)

    # Check partition spec on latency_samples
    lat_tbl = wh.latency_table
    assert any(f.name == "series" for f in lat_tbl.spec().fields)


def test_warehouse_load_run(in_memory_warehouse: IcebergWarehouse) -> None:
    wh = in_memory_warehouse
    doc = _sample_manifest("run-001", "kafka", valid=True)

    loaded = wh.load_run(doc)
    assert loaded is True
    assert wh.has_run("run-001") is True

    # Verify bench.runs contents
    runs = wh.query_runs()
    assert len(runs) == 1
    r = runs[0]
    assert r["run_id"] == "run-001"
    assert r["driver"] == "kafka"
    assert r["valid"] is True
    assert r["target_rate_hz"] == 10000.0
    assert r["achieved_rate_hz"] == 9985.0
    assert r["sent_total"] == 50000
    assert r["messages_received"] == 50000
    assert r["messages_missing"] == 0
    assert r["response_p50_us"] == 320.0
    assert r["response_p99_us"] == 1400.0

    # Verify latency_samples
    lat_rows = wh.latency_table.scan().to_arrow().to_pylist()
    assert len(lat_rows) == 2
    series_names = {row["series"] for row in lat_rows}
    assert series_names == {"response", "service"}

    # Verify throughput_series
    tp_rows = wh.throughput_table.scan().to_arrow().to_pylist()
    assert len(tp_rows) == 3
    assert [row["messages"] for row in tp_rows] == [9990, 9980, 9985]


def test_warehouse_idempotency(in_memory_warehouse: IcebergWarehouse) -> None:
    wh = in_memory_warehouse
    doc = _sample_manifest("run-idemp-001")

    # First load
    assert wh.load_run(doc) is True

    # Second load returns False (no-op)
    assert wh.load_run(doc) is False

    # Still exactly one record in runs
    assert len(wh.query_runs()) == 1

    # Fails if explicitly requested
    with pytest.raises(ValueError, match="already exists"):
        wh.load_run(doc, fail_if_exists=True)


def test_warehouse_supersedes_trail(in_memory_warehouse: IcebergWarehouse) -> None:
    """Invariant 5: results are append-only.

    Corrections append new runs referencing the supersedes_run_id.
    """
    wh = in_memory_warehouse
    doc1 = _sample_manifest("run-original", "pulsar", valid=False)
    doc2 = _sample_manifest("run-corrected", "pulsar", valid=True)

    wh.load_run(doc1)
    wh.load_run(doc2, supersedes_run_id="run-original")

    runs = {r["run_id"]: r for r in wh.query_runs()}
    assert len(runs) == 2
    assert runs["run-original"]["valid"] is False
    assert runs["run-original"]["supersedes_run_id"] is None
    assert runs["run-corrected"]["valid"] is True
    assert runs["run-corrected"]["supersedes_run_id"] == "run-original"


def test_warehouse_load_directory(
    in_memory_warehouse: IcebergWarehouse,
    tmp_path: pathlib.Path,
) -> None:
    wh = in_memory_warehouse
    res_dir = tmp_path / "results"
    res_dir.mkdir()

    r1_dir = res_dir / "run-dir-1"
    r1_dir.mkdir()
    (r1_dir / "manifest.json").write_text(
        json.dumps(_sample_manifest("run-dir-1", "kafka")),
        encoding="utf-8",
    )

    r2_dir = res_dir / "run-dir-2"
    r2_dir.mkdir()
    (r2_dir / "manifest.json").write_text(
        json.dumps(_sample_manifest("run-dir-2", "pulsar")),
        encoding="utf-8",
    )

    loaded = wh.load_directory(res_dir)
    assert set(loaded) == {"run-dir-1", "run-dir-2"}

    # Repeated scan is idempotent
    assert wh.load_directory(res_dir) == []


def test_warehouse_query_filtering(in_memory_warehouse: IcebergWarehouse) -> None:
    wh = in_memory_warehouse
    wh.load_run(_sample_manifest("run-k1", "kafka", valid=True))
    wh.load_run(_sample_manifest("run-k2", "kafka", valid=False))
    wh.load_run(_sample_manifest("run-p1", "pulsar", valid=True))

    assert len(wh.query_runs()) == 3
    assert len(wh.query_runs(driver="kafka")) == 2
    assert len(wh.query_runs(driver="pulsar")) == 1
    assert len(wh.query_runs(valid_only=True)) == 2
    assert len(wh.query_runs(driver="kafka", valid_only=True)) == 1


def test_warehouse_schema_evolution(in_memory_warehouse: IcebergWarehouse) -> None:
    """FR-10: schema evolution without rewriting historical runs."""
    wh = in_memory_warehouse
    wh.load_run(_sample_manifest("run-v1", "kafka"))

    # Add a new column to bench.runs
    from pyiceberg.types import StringType

    with wh.runs_table.update_schema() as update:
        update.add_column("cluster_topology", StringType(), doc="Topology metadata")

    # Verify historical run still scans cleanly and cluster_topology is None
    records = wh.runs_table.scan().to_arrow().to_pylist()
    assert len(records) == 1
    assert records[0]["run_id"] == "run-v1"
    assert records[0]["cluster_topology"] is None
