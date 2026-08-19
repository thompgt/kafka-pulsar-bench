"""Environment fingerprinting (requirement M-3).

A latency number without the environment that produced it is not a result. This
captures enough to tell, months later, whether two runs are comparable at all:
hardware, kernel, Docker, the exact image digests, and the commit of the
harness itself.

Image *digests* rather than tags, because tags move. A tag that pointed at one
build in August and another in October would make two runs look comparable when
they are not.
"""

from __future__ import annotations

import os
import pathlib
import platform
import shutil
import subprocess
from typing import Any


def _run(cmd: list[str], timeout: float = 15.0) -> str | None:
    exe = shutil.which(cmd[0])
    if exe is None:
        return None
    try:
        out = subprocess.run(
            [exe, *cmd[1:]],
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except (subprocess.TimeoutExpired, OSError):
        return None
    return out.stdout.strip() if out.returncode == 0 else None


def _cpu_model() -> str:
    # /proc/cpuinfo on Linux; platform.processor() is often empty there but is
    # the only thing available on Windows and macOS.
    cpuinfo = pathlib.Path("/proc/cpuinfo")
    if cpuinfo.exists():
        for line in cpuinfo.read_text(encoding="utf-8", errors="replace").splitlines():
            if line.startswith("model name"):
                return line.split(":", 1)[1].strip()
    return platform.processor() or "unknown"


def _total_memory_bytes() -> int | None:
    meminfo = pathlib.Path("/proc/meminfo")
    if meminfo.exists():
        for line in meminfo.read_text(encoding="utf-8", errors="replace").splitlines():
            if line.startswith("MemTotal:"):
                return int(line.split()[1]) * 1024
    return None


def _docker_info() -> dict[str, Any]:
    info: dict[str, Any] = {}
    version = _run(["docker", "version", "--format", "{{.Server.Version}}"])
    if version:
        info["server_version"] = version
    ncpu = _run(["docker", "info", "--format", "{{.NCPU}}"])
    if ncpu and ncpu.isdigit():
        info["ncpu"] = int(ncpu)
    memtotal = _run(["docker", "info", "--format", "{{.MemTotal}}"])
    if memtotal and memtotal.isdigit():
        info["mem_total_bytes"] = int(memtotal)
    return info


def _running_container_digests() -> dict[str, str]:
    """Digest of every running kpbench container.

    The digest of the image *actually running* is what matters, not what the
    lock file says should be running. Those two disagree precisely when
    something went wrong, which is when the fingerprint earns its keep.
    """
    out = _run(
        ["docker", "ps", "--filter", "name=kpbench-", "--format", "{{.Names}}\t{{.Image}}"]
    )
    if not out:
        return {}
    digests: dict[str, str] = {}
    for line in out.splitlines():
        parts = line.split("\t")
        if len(parts) != 2:
            continue
        name, image = parts
        repo_digest = _run(
            ["docker", "inspect", "--format", "{{index .RepoDigests 0}}", name]
        )
        digests[name] = repo_digest or image
    return digests


def _image_lock(repo_root: pathlib.Path) -> dict[str, str]:
    lock = repo_root / "infra" / "compose" / "images.lock"
    if not lock.exists():
        return {}
    entries: dict[str, str] = {}
    for line in lock.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) == 2:
            entries[parts[0]] = parts[1]
    return entries


def _git_commit(repo_root: pathlib.Path) -> dict[str, str]:
    info: dict[str, str] = {}
    commit = _run(["git", "-C", str(repo_root), "rev-parse", "HEAD"])
    if commit:
        info["commit"] = commit
    dirty = _run(["git", "-C", str(repo_root), "status", "--porcelain"])
    # An uncommitted working tree means the result cannot be reproduced from
    # any commit, which is worth recording rather than discovering later.
    info["dirty"] = "true" if dirty else "false"
    return info


def repo_root() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parents[4]


def collect() -> dict[str, Any]:
    root = repo_root()
    return {
        "host": {
            "platform": platform.platform(),
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
            "cpu_model": _cpu_model(),
            "cpu_count": os.cpu_count(),
            "memory_bytes": _total_memory_bytes(),
            "python": platform.python_version(),
            "python_impl": platform.python_implementation(),
        },
        "docker": _docker_info(),
        "container_digests": _running_container_digests(),
        "image_lock": _image_lock(root),
        "git": _git_commit(root),
    }
