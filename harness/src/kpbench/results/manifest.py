"""Run manifest (FR-5).

One JSON file per run holding everything needed to interpret, reproduce, or
reject it: the config that produced it, the environment it ran in, the client
libraries involved, the metrics, and the validity verdict with its reasons.

The validity verdict is written into the manifest rather than being applied by
filtering runs out. An invalid run is still evidence — it usually records the
point at which the harness or the broker ran out of headroom — it just may not
be reported as a latency result (M-8).
"""

from __future__ import annotations

import json
import pathlib
from typing import Any

from kpbench.config import RunConfig
from kpbench.workload.runner import RunOutcome

MANIFEST_VERSION = 1


def build(
    outcome: RunOutcome,
    config: RunConfig,
    environment: dict[str, Any],
    client_info: dict[str, str],
) -> dict[str, Any]:
    return {
        "manifest_version": MANIFEST_VERSION,
        "run_id": outcome.run_id,
        "valid": outcome.valid,
        "invalid_reasons": outcome.reasons,
        "driver": config.driver,
        "config": config.model_dump(mode="json"),
        "client": client_info,
        "environment": environment,
        "metrics": outcome.metrics,
    }


def write(manifest: dict[str, Any], results_dir: pathlib.Path) -> pathlib.Path:
    run_dir = results_dir / str(manifest["run_id"])
    run_dir.mkdir(parents=True, exist_ok=True)
    path = run_dir / "manifest.json"
    path.write_text(json.dumps(manifest, indent=2, sort_keys=False), encoding="utf-8")
    return path


def read(path: str | pathlib.Path) -> dict[str, Any]:
    data: Any = json.loads(pathlib.Path(path).read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"{path}: manifest must be a JSON object")
    return data
