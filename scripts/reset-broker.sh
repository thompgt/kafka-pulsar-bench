#!/usr/bin/env bash
# Destroy and rebuild the broker, volume included, leaving the warehouse alone.
#
# This is invariant 4 made operational. Without it, run N inherits run N-1's
# page cache, segment layout and compaction debt, and a sweep silently measures
# accumulated state rather than the configuration it claims to test.
#
# Called between every point of a sweep. The warehouse must survive, because
# it holds the results of the runs already completed.

set -euo pipefail

PROFILE="${1:-kafka}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_DIR="${REPO_ROOT}/infra/compose"

if [[ "${PROFILE}" != "kafka" && "${PROFILE}" != "pulsar" ]]; then
  echo "ERROR: reset-broker needs a broker profile (kafka|pulsar), got '${PROFILE}'" >&2
  exit 1
fi

ENV_ARGS=(--env-file "${COMPOSE_DIR}/versions.env")
[[ -f "${COMPOSE_DIR}/local.env" ]] && ENV_ARGS+=(--env-file "${COMPOSE_DIR}/local.env")
FILE_ARGS=(-f "${COMPOSE_DIR}/docker-compose.core.yml" -f "${COMPOSE_DIR}/docker-compose.${PROFILE}.yml")

VOLUME="kpbench_${PROFILE}_data"

echo "resetting broker: ${PROFILE}"

# Stop and remove only the broker service. `down` would take core with it.
docker compose "${ENV_ARGS[@]}" "${FILE_ARGS[@]}" rm --stop --force --volumes "${PROFILE}" >/dev/null 2>&1 || true

# `rm --volumes` only removes anonymous volumes, so the named one goes explicitly.
if docker volume inspect "${VOLUME}" >/dev/null 2>&1; then
  docker volume rm "${VOLUME}" >/dev/null
  echo "removed volume ${VOLUME}"
fi

# Recreate through up.sh so the readiness wait applies here too.
exec "${REPO_ROOT}/scripts/up.sh" "${PROFILE}"
