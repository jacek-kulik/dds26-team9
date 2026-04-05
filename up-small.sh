#!/usr/bin/env bash
set -euo pipefail

# Start the local Docker Compose stack with default replica counts.
# Override any value by exporting env vars before running, e.g.:
#   USER_WORKER_REPLICAS=6 ./up.sh
# Set DETACH=1 to run in the background (-d).
# Use DRY_RUN=1 to print the command without executing it.

COMPOSE_FILE="docker-compose-small.yml"

ORDER_WEB_REPLICAS="${ORDER_WEB_REPLICAS:-6}"
ORDER_WORKER_REPLICAS="${ORDER_WORKER_REPLICAS:-2}"
ORCH_WORKER_REPLICAS="${ORCH_WORKER_REPLICAS:-6}"
STOCK_WEB_REPLICAS="${STOCK_WEB_REPLICAS:-1}"
STOCK_WORKER_REPLICAS="${STOCK_WORKER_REPLICAS:-2}"
USER_WEB_REPLICAS="${USER_WEB_REPLICAS:-1}"
USER_WORKER_REPLICAS="${USER_WORKER_REPLICAS:-1}"
DETACH="${DETACH:-1}"

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

build_cmd=(
  docker compose -f "$COMPOSE_FILE" build --no-cache
)

up_cmd=(
  docker compose -f "$COMPOSE_FILE" up
  --scale "order-web=${ORDER_WEB_REPLICAS}"
  --scale "order-worker=${ORDER_WORKER_REPLICAS}"
  --scale "orchestrator-worker=${ORCH_WORKER_REPLICAS}"
  --scale "stock-web=${STOCK_WEB_REPLICAS}"
  --scale "stock-worker=${STOCK_WORKER_REPLICAS}"
  --scale "user-web=${USER_WEB_REPLICAS}"
  --scale "user-worker=${USER_WORKER_REPLICAS}"
)

if [[ "${DETACH}" == "1" ]]; then
  up_cmd+=( -d )
fi


if [[ "${DRY_RUN:-0}" == "1" ]]; then
  printf 'DRY RUN BUILD: '
  printf '%q ' "${build_cmd[@]}"
  printf '\n'
  printf 'DRY RUN UP: '
  printf '%q ' "${up_cmd[@]}"
  printf '\n'
  exit 0
fi

docker compose down --remove-orphans

"${build_cmd[@]}"
"${up_cmd[@]}"


