"""HTTP server providing REST APIs and static UI for the kpbench dashboard."""

from __future__ import annotations

import http.server
import json
import mimetypes
import pathlib
import threading
from typing import Any

from kpbench.results import manifest as manifest_mod

STATIC_DIR = pathlib.Path(__file__).parent / "static"


def list_runs(results_dir: pathlib.Path) -> list[dict[str, Any]]:
    """Scan results_dir for manifests and return summary cards."""
    runs: list[dict[str, Any]] = []
    if not results_dir.exists():
        return runs

    for run_dir in sorted(results_dir.iterdir(), reverse=True):
        if not run_dir.is_dir():
            continue
        manifest_path = run_dir / "manifest.json"
        if not manifest_path.exists():
            continue
        try:
            doc = manifest_mod.read(manifest_path)
            m = doc.get("metrics", {})
            resp = m.get("latency", {}).get("response", {}).get("percentiles_us", {})
            runs.append({
                "run_id": doc.get("run_id", run_dir.name),
                "driver": doc.get("driver", "unknown"),
                "valid": doc.get("valid", False),
                "invalid_reasons": doc.get("invalid_reasons", []),
                "warmup_run": doc.get("warmup_run", False),
                "target_rate_hz": m.get("target_rate_hz", 0),
                "achieved_rate_hz": m.get("achieved_rate_hz", 0),
                "achieved_rate_ratio": m.get("achieved_rate_ratio", 0.0),
                "p50_us": resp.get("p50", 0),
                "p99_us": resp.get("p99", 0),
                "p99_9_us": resp.get("p99.9", 0),
                "started_at": m.get("started_at", ""),
                "message_bytes": doc.get("config", {}).get("workload", {}).get("message_bytes", 0),
                "partitions": doc.get("config", {}).get("topic", {}).get("partitions", 1),
            })
        except Exception:
            continue

    return runs


def get_run_details(results_dir: pathlib.Path, run_id: str) -> dict[str, Any] | None:
    """Read full manifest for run_id."""
    manifest_path = results_dir / run_id / "manifest.json"
    if not manifest_path.exists():
        return None
    try:
        return manifest_mod.read(manifest_path)
    except Exception:
        return None


class DashboardHandler(http.server.BaseHTTPRequestHandler):
    results_dir: pathlib.Path = pathlib.Path("results")

    def _send_json(self, data: Any, status: int = 200) -> None:
        body = json.dumps(data, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _send_file(self, filepath: pathlib.Path) -> None:
        if not filepath.exists() or not filepath.is_file():
            self.send_response(404)
            self.end_headers()
            return
        mime_type, _ = mimetypes.guess_type(str(filepath))
        content = filepath.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", mime_type or "application/octet-stream")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def do_GET(self) -> None:
        path = self.path.split("?")[0].rstrip("/")
        if not path:
            path = "/"

        # REST APIs
        if path == "/api/health":
            self._send_json({"status": "healthy", "service": "kpbench-dashboard"})
            return

        if path == "/api/runs":
            self._send_json(list_runs(self.results_dir))
            return

        if path.startswith("/api/runs/"):
            run_id = path.removeprefix("/api/runs/")
            details = get_run_details(self.results_dir, run_id)
            if details is None:
                self._send_json({"error": f"run '{run_id}' not found"}, 404)
            else:
                self._send_json(details)
            return

        # Static assets
        if path == "/":
            self._send_file(STATIC_DIR / "index.html")
            return

        candidate = STATIC_DIR / path.lstrip("/")
        if candidate.exists() and candidate.is_file():
            self._send_file(candidate)
        else:
            # Fallback to index.html for SPA routing
            self._send_file(STATIC_DIR / "index.html")

    def log_message(self, format: str, *args: Any) -> None:
        pass


class DashboardServer:
    """HTTP server providing REST APIs and static UI for the dashboard."""

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 8050,
        results_dir: str | pathlib.Path = "results",
    ) -> None:
        self.host = host
        self.results_dir = pathlib.Path(results_dir)
        handler_cls = type(
            "ConfiguredDashboardHandler",
            (DashboardHandler,),
            {"results_dir": self.results_dir},
        )
        self.server = http.server.HTTPServer((self.host, port), handler_cls)
        self.port: int = self.server.server_address[1]
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=2.0)


def run_server(
    host: str = "0.0.0.0",
    port: int = 8050,
    results_dir: str | pathlib.Path = "results",
) -> None:
    """Launch the dashboard HTTP server."""
    dash = DashboardServer(host=host, port=port, results_dir=results_dir)
    print(f"  kpbench dashboard running at http://localhost:{dash.port}/")
    print(f"  serving manifests from: {dash.results_dir.resolve()}")
    try:
        dash.server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        dash.stop()

