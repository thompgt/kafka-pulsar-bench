"""Results warehouse integration using Apache Iceberg and MinIO S3.

Persists benchmark manifests, latency samples (with encoded HdrHistograms for
lossless merging), and throughput series into durable, queryable Iceberg tables
via the REST catalog.
"""

from __future__ import annotations

import datetime
import json
import os
import pathlib
from typing import Any

import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.expressions import EqualTo, Reference
from pyiceberg.expressions.literals import literal
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

from kpbench.results import manifest as manifest_mod

DEFAULT_CATALOG_URL = os.environ.get("ICEBERG_CATALOG_URL", "http://localhost:8181")
DEFAULT_S3_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
DEFAULT_S3_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "minioadmin")
DEFAULT_S3_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")
DEFAULT_WAREHOUSE_BUCKET = os.environ.get("WAREHOUSE_BUCKET", "warehouse")
DEFAULT_NAMESPACE = "bench"

# ---------------------------------------------------------------------------
# Iceberg Schemas (All optional to allow robust evolution without schema rewrites)
# ---------------------------------------------------------------------------

RUNS_SCHEMA = Schema(
    NestedField(field_id=1, name="run_id", field_type=StringType(), required=False),
    NestedField(field_id=2, name="created_at", field_type=TimestamptzType(), required=False),
    NestedField(field_id=3, name="driver", field_type=StringType(), required=False),
    NestedField(field_id=4, name="valid", field_type=BooleanType(), required=False),
    NestedField(field_id=5, name="invalid_reasons", field_type=StringType(), required=False),
    NestedField(field_id=6, name="warmup_run", field_type=BooleanType(), required=False),
    NestedField(field_id=7, name="config_json", field_type=StringType(), required=False),
    NestedField(field_id=8, name="environment_json", field_type=StringType(), required=False),
    NestedField(field_id=9, name="client_json", field_type=StringType(), required=False),
    NestedField(field_id=10, name="target_rate_hz", field_type=DoubleType(), required=False),
    NestedField(field_id=11, name="achieved_rate_hz", field_type=DoubleType(), required=False),
    NestedField(field_id=12, name="achieved_rate_ratio", field_type=DoubleType(), required=False),
    NestedField(field_id=13, name="sent_total", field_type=LongType(), required=False),
    NestedField(field_id=14, name="sent_measured", field_type=LongType(), required=False),
    NestedField(field_id=15, name="messages_received", field_type=LongType(), required=False),
    NestedField(field_id=16, name="messages_missing", field_type=LongType(), required=False),
    NestedField(field_id=17, name="messages_duplicates", field_type=LongType(), required=False),
    NestedField(field_id=18, name="messages_out_of_order", field_type=LongType(), required=False),
    NestedField(field_id=19, name="response_p50_us", field_type=DoubleType(), required=False),
    NestedField(field_id=20, name="response_p99_us", field_type=DoubleType(), required=False),
    NestedField(field_id=21, name="response_p99_9_us", field_type=DoubleType(), required=False),
    NestedField(field_id=22, name="response_max_us", field_type=DoubleType(), required=False),
    NestedField(field_id=23, name="supersedes_run_id", field_type=StringType(), required=False),
)

RUNS_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=3, field_id=1000, transform=IdentityTransform(), name="driver"),
)

LATENCY_SAMPLES_SCHEMA = Schema(
    NestedField(field_id=1, name="run_id", field_type=StringType(), required=False),
    NestedField(field_id=2, name="series", field_type=StringType(), required=False),
    NestedField(field_id=3, name="count", field_type=LongType(), required=False),
    NestedField(field_id=4, name="min_us", field_type=LongType(), required=False),
    NestedField(field_id=5, name="max_us", field_type=LongType(), required=False),
    NestedField(field_id=6, name="mean_us", field_type=DoubleType(), required=False),
    NestedField(field_id=7, name="p50_us", field_type=DoubleType(), required=False),
    NestedField(field_id=8, name="p75_us", field_type=DoubleType(), required=False),
    NestedField(field_id=9, name="p90_us", field_type=DoubleType(), required=False),
    NestedField(field_id=10, name="p95_us", field_type=DoubleType(), required=False),
    NestedField(field_id=11, name="p99_us", field_type=DoubleType(), required=False),
    NestedField(field_id=12, name="p99_9_us", field_type=DoubleType(), required=False),
    NestedField(field_id=13, name="p99_99_us", field_type=DoubleType(), required=False),
    NestedField(field_id=14, name="encoded_histogram", field_type=StringType(), required=False),
)

LATENCY_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="series"),
)

THROUGHPUT_SERIES_SCHEMA = Schema(
    NestedField(field_id=1, name="run_id", field_type=StringType(), required=False),
    NestedField(field_id=2, name="second", field_type=IntegerType(), required=False),
    NestedField(field_id=3, name="messages", field_type=IntegerType(), required=False),
)

PA_RUNS_SCHEMA = pa.schema([
    ("run_id", pa.string()),
    ("created_at", pa.timestamp("us", tz="UTC")),
    ("driver", pa.string()),
    ("valid", pa.bool_()),
    ("invalid_reasons", pa.string()),
    ("warmup_run", pa.bool_()),
    ("config_json", pa.string()),
    ("environment_json", pa.string()),
    ("client_json", pa.string()),
    ("target_rate_hz", pa.float64()),
    ("achieved_rate_hz", pa.float64()),
    ("achieved_rate_ratio", pa.float64()),
    ("sent_total", pa.int64()),
    ("sent_measured", pa.int64()),
    ("messages_received", pa.int64()),
    ("messages_missing", pa.int64()),
    ("messages_duplicates", pa.int64()),
    ("messages_out_of_order", pa.int64()),
    ("response_p50_us", pa.float64()),
    ("response_p99_us", pa.float64()),
    ("response_p99_9_us", pa.float64()),
    ("response_max_us", pa.float64()),
    ("supersedes_run_id", pa.string()),
])

PA_LATENCY_SCHEMA = pa.schema([
    ("run_id", pa.string()),
    ("series", pa.string()),
    ("count", pa.int64()),
    ("min_us", pa.int64()),
    ("max_us", pa.int64()),
    ("mean_us", pa.float64()),
    ("p50_us", pa.float64()),
    ("p75_us", pa.float64()),
    ("p90_us", pa.float64()),
    ("p95_us", pa.float64()),
    ("p99_us", pa.float64()),
    ("p99_9_us", pa.float64()),
    ("p99_99_us", pa.float64()),
    ("encoded_histogram", pa.string()),
])

PA_THROUGHPUT_SCHEMA = pa.schema([
    ("run_id", pa.string()),
    ("second", pa.int32()),
    ("messages", pa.int32()),
])


class IcebergWarehouse:
    """Manages results ingestion and querying in Apache Iceberg."""

    def __init__(
        self,
        catalog: Catalog | None = None,
        catalog_url: str = DEFAULT_CATALOG_URL,
        s3_endpoint: str = DEFAULT_S3_ENDPOINT,
        s3_access_key: str = DEFAULT_S3_ACCESS_KEY,
        s3_secret_key: str = DEFAULT_S3_SECRET_KEY,
        warehouse_bucket: str = DEFAULT_WAREHOUSE_BUCKET,
        namespace: str = DEFAULT_NAMESPACE,
    ) -> None:
        self.namespace = namespace
        if catalog is not None:
            self.catalog = catalog
        else:
            self.catalog = load_catalog(
                "bench_catalog",
                **{
                    "type": "rest",
                    "uri": catalog_url,
                    "s3.endpoint": s3_endpoint,
                    "s3.access-key-id": s3_access_key,
                    "s3.secret-access-key": s3_secret_key,
                    "s3.path-style-access": "true",
                    "warehouse": f"s3://{warehouse_bucket}/",
                },
            )
        self.ensure_tables()

    def ensure_tables(self) -> None:
        """Create namespace and benchmark tables if not already existing."""
        try:
            self.catalog.create_namespace_if_not_exists(self.namespace)
        except Exception:
            # Some catalog implementations require namespace check first
            existing = [ns[0] for ns in self.catalog.list_namespaces()]
            if self.namespace not in existing:
                self.catalog.create_namespace(self.namespace)

        self.runs_table: Table = self.catalog.create_table_if_not_exists(
            f"{self.namespace}.runs",
            schema=RUNS_SCHEMA,
            partition_spec=RUNS_PARTITION_SPEC,
        )
        self.latency_table: Table = self.catalog.create_table_if_not_exists(
            f"{self.namespace}.latency_samples",
            schema=LATENCY_SAMPLES_SCHEMA,
            partition_spec=LATENCY_PARTITION_SPEC,
        )
        self.throughput_table: Table = self.catalog.create_table_if_not_exists(
            f"{self.namespace}.throughput_series",
            schema=THROUGHPUT_SERIES_SCHEMA,
        )

    def has_run(self, run_id: str) -> bool:
        """Check whether run_id is already loaded in bench.runs."""
        res = self.runs_table.scan(
            row_filter=EqualTo(term=Reference("run_id"), value=literal(run_id))
        ).to_arrow()
        return len(res) > 0

    def load_run(
        self,
        manifest_doc: dict[str, Any],
        supersedes_run_id: str | None = None,
        fail_if_exists: bool = False,
    ) -> bool:
        """Ingest a benchmark run manifest into Iceberg.

        Idempotent: skips if run_id already exists unless fail_if_exists is True.
        Enforces append-only: never modifies existing records (Invariant 5).
        """
        run_id = manifest_doc.get("run_id", "")
        if not run_id:
            raise ValueError("Manifest missing required 'run_id'")

        if self.has_run(run_id):
            if fail_if_exists:
                raise ValueError(f"Run '{run_id}' already exists in warehouse")
            return False

        m = manifest_doc.get("metrics", {})
        started_str = m.get("started_at")
        if started_str:
            try:
                created_at = datetime.datetime.fromisoformat(started_str)
                if created_at.tzinfo is None:
                    created_at = created_at.replace(tzinfo=datetime.UTC)
            except Exception:
                created_at = datetime.datetime.now(datetime.UTC)
        else:
            created_at = datetime.datetime.now(datetime.UTC)

        delivery = m.get("delivery", {})
        resp = m.get("latency", {}).get("response", {})
        resp_pct = resp.get("percentiles_us", {})

        # 1. bench.runs record
        runs_data = {
            "run_id": [run_id],
            "created_at": pa.array([created_at], type=pa.timestamp("us", tz="UTC")),
            "driver": [manifest_doc.get("driver", "unknown")],
            "valid": [manifest_doc.get("valid", False)],
            "invalid_reasons": [json.dumps(manifest_doc.get("invalid_reasons", []))],
            "warmup_run": [manifest_doc.get("warmup_run", False)],
            "config_json": [json.dumps(manifest_doc.get("config", {}))],
            "environment_json": [json.dumps(manifest_doc.get("environment", {}))],
            "client_json": [json.dumps(manifest_doc.get("client", {}))],
            "target_rate_hz": [float(m.get("target_rate_hz", 0.0))],
            "achieved_rate_hz": [float(m.get("achieved_rate_hz", 0.0))],
            "achieved_rate_ratio": [float(m.get("achieved_rate_ratio", 0.0))],
            "sent_total": [int(m.get("sent_total", 0))],
            "sent_measured": [int(m.get("sent_measured", 0))],
            "messages_received": [int(delivery.get("received", 0))],
            "messages_missing": [int(delivery.get("missing", 0))],
            "messages_duplicates": [int(delivery.get("duplicates", 0))],
            "messages_out_of_order": [int(delivery.get("out_of_order", 0))],
            "response_p50_us": [float(resp_pct.get("p50", 0.0))],
            "response_p99_us": [float(resp_pct.get("p99", 0.0))],
            "response_p99_9_us": [float(resp_pct.get("p99.9", 0.0))],
            "response_max_us": [float(resp.get("max_us", 0.0))],
            "supersedes_run_id": [supersedes_run_id],
        }
        self.runs_table.append(pa.Table.from_pydict(runs_data, schema=PA_RUNS_SCHEMA))

        # 2. bench.latency_samples records
        latencies = m.get("latency", {})
        if latencies:
            lat_rows: dict[str, list[Any]] = {
                "run_id": [],
                "series": [],
                "count": [],
                "min_us": [],
                "max_us": [],
                "mean_us": [],
                "p50_us": [],
                "p75_us": [],
                "p90_us": [],
                "p95_us": [],
                "p99_us": [],
                "p99_9_us": [],
                "p99_99_us": [],
                "encoded_histogram": [],
            }
            for series_name, s_data in latencies.items():
                pcts = s_data.get("percentiles_us", {})
                lat_rows["run_id"].append(run_id)
                lat_rows["series"].append(series_name)
                lat_rows["count"].append(int(s_data.get("count", 0)))
                lat_rows["min_us"].append(int(s_data.get("min_us", 0)))
                lat_rows["max_us"].append(int(s_data.get("max_us", 0)))
                lat_rows["mean_us"].append(float(s_data.get("mean_us", 0.0)))
                lat_rows["p50_us"].append(float(pcts.get("p50", 0.0)))
                lat_rows["p75_us"].append(float(pcts.get("p75", 0.0)))
                lat_rows["p90_us"].append(float(pcts.get("p90", 0.0)))
                lat_rows["p95_us"].append(float(pcts.get("p95", 0.0)))
                lat_rows["p99_us"].append(float(pcts.get("p99", 0.0)))
                lat_rows["p99_9_us"].append(float(pcts.get("p99.9", 0.0)))
                lat_rows["p99_99_us"].append(float(pcts.get("p99.99", 0.0)))
                lat_rows["encoded_histogram"].append(str(s_data.get("encoded", "")))

            self.latency_table.append(pa.Table.from_pydict(lat_rows, schema=PA_LATENCY_SCHEMA))

        # 3. bench.throughput_series records
        tp_series = m.get("throughput_series", [])
        if tp_series:
            tp_rows = {
                "run_id": [run_id] * len(tp_series),
                "second": [int(item.get("second", 0)) for item in tp_series],
                "messages": [int(item.get("messages", 0)) for item in tp_series],
            }
            self.throughput_table.append(pa.Table.from_pydict(tp_rows, schema=PA_THROUGHPUT_SCHEMA))

        return True

    def load_manifest_file(
        self,
        filepath: pathlib.Path | str,
        supersedes_run_id: str | None = None,
    ) -> bool:
        """Load manifest from file path."""
        doc = manifest_mod.read(filepath)
        return self.load_run(doc, supersedes_run_id=supersedes_run_id)

    def load_directory(self, results_dir: pathlib.Path | str) -> list[str]:
        """Scan directory and load all manifests idempotently."""
        p = pathlib.Path(results_dir)
        loaded: list[str] = []
        if not p.exists():
            return loaded

        # Search recursively for manifest.json files
        manifests = sorted(p.glob("**/manifest.json"))
        for mf in manifests:
            try:
                doc = manifest_mod.read(mf)
                run_id = doc.get("run_id", "")
                if run_id and self.load_run(doc):
                    loaded.append(run_id)
            except Exception:
                continue

        return loaded

    def query_runs(
        self,
        driver: str | None = None,
        valid_only: bool = False,
    ) -> list[dict[str, Any]]:
        """Query runs from bench.runs."""
        scan = self.runs_table.scan()
        if driver:
            scan = scan.filter(EqualTo(term=Reference("driver"), value=literal(driver)))
        if valid_only:
            scan = scan.filter(EqualTo(term=Reference("valid"), value=literal(True)))

        df = scan.to_arrow()
        rows: list[dict[str, Any]] = df.to_pylist()
        return rows
