#!/usr/bin/env bash
# Fail fast on the environment problems that otherwise show up as confusing
# container crashes ten minutes into a run.
#
# Memory is the one that matters most: an under-provisioned Docker VM does not
# fail cleanly, it produces slow and noisy results, which is worse than an
# error because the numbers still look plausible.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VERSIONS_FILE="${REPO_ROOT}/infra/compose/versions.env"

RED=$'\033[31m'; YELLOW=$'\033[33m'; GREEN=$'\033[32m'; RESET=$'\033[0m'
warnings=0
errors=0

ok()   { echo "  ${GREEN}ok${RESET}      $*"; }
warn() { echo "  ${YELLOW}warn${RESET}    $*"; warnings=$((warnings + 1)); }
err()  { echo "  ${RED}error${RESET}   $*"; errors=$((errors + 1)); }

echo "preflight"

# --- Docker present and responsive --------------------------------------
if ! command -v docker >/dev/null 2>&1; then
  err "docker not found on PATH"
  exit 1
fi
if ! docker info >/dev/null 2>&1; then
  err "docker daemon not responding (is Docker Desktop running?)"
  exit 1
fi
ok "docker daemon responding"

# --- Memory available to the Docker VM ----------------------------------
# NFR-1 budgets 16 GB total; a single broker profile needs roughly 6 GB.
MEM_BYTES="$(docker info --format '{{.MemTotal}}' 2>/dev/null || echo 0)"
MEM_GB=$(( MEM_BYTES / 1024 / 1024 / 1024 ))
if   [[ "${MEM_GB}" -lt 6 ]]; then
  err "docker has ${MEM_GB}GB; a broker profile needs ~6GB. Raise the limit in .wslconfig"
elif [[ "${MEM_GB}" -lt 10 ]]; then
  warn "docker has ${MEM_GB}GB; enough for one profile, not for engines alongside it"
else
  ok "docker memory: ${MEM_GB}GB"
fi

# --- CPU ----------------------------------------------------------------
CPUS="$(docker info --format '{{.NCPU}}' 2>/dev/null || echo 0)"
if [[ "${CPUS}" -lt 4 ]]; then
  warn "docker has ${CPUS} CPUs; the harness and broker will contend, adding jitter"
else
  ok "docker cpus: ${CPUS}"
fi

# --- Disk ---------------------------------------------------------------
AVAIL_KB="$(df -Pk "${REPO_ROOT}" | awk 'NR==2 {print $4}')"
AVAIL_GB=$(( AVAIL_KB / 1024 / 1024 ))
if   [[ "${AVAIL_GB}" -lt 10 ]]; then
  err "only ${AVAIL_GB}GB free; images alone need ~10GB"
elif [[ "${AVAIL_GB}" -lt 25 ]]; then
  warn "${AVAIL_GB}GB free; sweeps accumulate warehouse data quickly"
else
  ok "disk free: ${AVAIL_GB}GB"
fi

# --- Port conflicts -----------------------------------------------------
# shellcheck disable=SC1090
set -a; source "${VERSIONS_FILE}"; set +a

port_in_use() {
  # Prefer ss, fall back to netstat; both are absent often enough to matter.
  if command -v ss >/dev/null 2>&1; then
    ss -ltn 2>/dev/null | awk '{print $4}' | grep -qE "[:.]$1\$"
  elif command -v netstat >/dev/null 2>&1; then
    netstat -an 2>/dev/null | grep -qE "[:.]$1[[:space:]].*LISTEN"
  else
    return 1
  fi
}

# Host ports published by our own containers. Built with `docker port` rather
# than by parsing `docker ps`, because ps renders contiguous mappings as a
# range ("9000-9001->9000-9001") that a per-port match silently misses.
OWN_PORTS=""
for name in $(docker ps --format '{{.Names}}' 2>/dev/null | grep '^kpbench-' || true); do
  OWN_PORTS+="$(docker port "${name}" 2>/dev/null | sed 's/.*:\([0-9]\+\)$/\1/') "
done

check_port() {
  local port="$1" what="$2"
  if port_in_use "${port}"; then
    # Our own containers holding the port is fine; anything else is not.
    if grep -qw "${port}" <<< "${OWN_PORTS}"; then
      ok "port ${port} (${what}) held by a kpbench container"
    else
      err "port ${port} (${what}) already in use by something else"
    fi
  else
    ok "port ${port} (${what}) free"
  fi
}

check_port "${MINIO_API_PORT:-9000}"      "minio api"
check_port "${MINIO_CONSOLE_PORT:-9001}"  "minio console"
check_port "${ICEBERG_REST_PORT:-8181}"   "iceberg rest"
check_port "${KAFKA_EXTERNAL_PORT:-29092}" "kafka"
check_port "${PULSAR_BROKER_PORT:-6650}"  "pulsar broker"
check_port "${PULSAR_ADMIN_PORT:-8080}"   "pulsar admin"

# --- Foreign containers --------------------------------------------------
# Anything else running on this Docker host competes for CPU and page cache
# with the broker under test. It does not cause a failure, it causes quietly
# worse and less repeatable numbers, which is the harder problem.
FOREIGN="$(docker ps --format '{{.Names}}' 2>/dev/null | grep -cv '^kpbench-' || true)"
if [[ "${FOREIGN}" -gt 0 ]]; then
  if [[ "${STRICT:-0}" == "1" ]]; then
    err "${FOREIGN} unrelated container(s) running; STRICT=1 requires an idle host"
    docker ps --format '    {{.Names}}' | grep -v '^    kpbench-' >&2 || true
  else
    warn "${FOREIGN} unrelated container(s) running; they contend for CPU and page cache"
    warn "stop them before a publishable run, or set STRICT=1 to make this an error"
  fi
else
  ok "no unrelated containers running"
fi

# --- Platform -----------------------------------------------------------
# Bind-mount performance across the Windows filesystem boundary is bad enough
# to perturb measurements, so this is a correctness warning, not a style note.
if [[ "$(uname -s)" == MINGW* || "$(uname -s)" == MSYS* ]]; then
  warn "running under Git Bash on Windows; run benchmarks from WSL2 instead"
fi

echo
if [[ "${errors}" -gt 0 ]]; then
  echo "${RED}preflight failed${RESET}: ${errors} error(s), ${warnings} warning(s)"
  exit 1
fi
echo "${GREEN}preflight passed${RESET}${warnings:+ (${warnings} warning(s))}"
