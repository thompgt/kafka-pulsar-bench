#!/usr/bin/env bash
# Bring up core plus one broker and block until everything is genuinely ready.
#
# "Genuinely ready" is the point of this script. `docker compose up -d` returns
# as soon as containers are started, and a benchmark that begins against a
# broker still loading its metadata produces a contaminated first minute that
# is very hard to spot afterwards.

set -euo pipefail

PROFILE="${1:-kafka}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_DIR="${REPO_ROOT}/infra/compose"

ENV_ARGS=(--env-file "${COMPOSE_DIR}/versions.env")
[[ -f "${COMPOSE_DIR}/local.env" ]] && ENV_ARGS+=(--env-file "${COMPOSE_DIR}/local.env")

FILE_ARGS=(-f "${COMPOSE_DIR}/docker-compose.core.yml")
[[ "${PROFILE}" != "none" ]] && FILE_ARGS+=(-f "${COMPOSE_DIR}/docker-compose.${PROFILE}.yml")

READY_TIMEOUT="${READY_TIMEOUT:-180}"

echo "starting profile: ${PROFILE}"
docker compose "${ENV_ARGS[@]}" "${FILE_ARGS[@]}" up -d --remove-orphans

# Only services that declare a healthcheck are waited on. One-shot bootstrap
# containers exit 0 and are handled by compose's dependency conditions.
mapfile -t CONTAINERS < <(docker compose "${ENV_ARGS[@]}" "${FILE_ARGS[@]}" ps -q)

echo "waiting for health (timeout ${READY_TIMEOUT}s)"
deadline=$(( SECONDS + READY_TIMEOUT ))

while :; do
  pending=()
  for cid in "${CONTAINERS[@]}"; do
    [[ -z "${cid}" ]] && continue
    name="$(docker inspect -f '{{.Name}}' "${cid}" 2>/dev/null | sed 's|^/||')"
    status="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "${cid}" 2>/dev/null || echo gone)"
    case "${status}" in
      healthy|exited|running) ;;   # 'running' = no healthcheck declared
      *) pending+=("${name}:${status}") ;;
    esac
  done

  [[ "${#pending[@]}" -eq 0 ]] && break

  if [[ "${SECONDS}" -ge "${deadline}" ]]; then
    echo "TIMED OUT waiting for: ${pending[*]}" >&2
    echo "recent logs:" >&2
    docker compose "${ENV_ARGS[@]}" "${FILE_ARGS[@]}" logs --tail=40 >&2
    exit 1
  fi
  sleep 3
done

echo
docker compose "${ENV_ARGS[@]}" "${FILE_ARGS[@]}" ps
echo
echo "ready."
