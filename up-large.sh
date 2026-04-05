#!/usr/bin/env bash
set -euo pipefail

ORDER_WEB_REPLICAS="${ORDER_WEB_REPLICAS:-20}"
ORDER_WORKER_REPLICAS="${ORDER_WORKER_REPLICAS:-16}"
STOCK_WEB_REPLICAS="${STOCK_WEB_REPLICAS:-5}"
STOCK_WORKER_REPLICAS="${STOCK_WORKER_REPLICAS:-8}"
USER_WEB_REPLICAS="${USER_WEB_REPLICAS:-5}"
USER_WORKER_REPLICAS="${USER_WORKER_REPLICAS:-8}"
DETACH="${DETACH:-1}"

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

cmd=(
  docker compose up --build
  --scale "order-web=${ORDER_WEB_REPLICAS}"
  --scale "order-worker=${ORDER_WORKER_REPLICAS}"
  --scale "stock-web=${STOCK_WEB_REPLICAS}"
  --scale "stock-worker=${STOCK_WORKER_REPLICAS}"
  --scale "user-web=${USER_WEB_REPLICAS}"
  --scale "user-worker=${USER_WORKER_REPLICAS}"
)

if [[ "${DETACH}" == "1" ]]; then
  cmd+=( -d )
fi

if [[ "${DRY_RUN:-0}" == "1" ]]; then
  printf 'DRY RUN: '
  printf '%q ' "${cmd[@]}"
  printf '\n'
  exit 0
fi

docker compose down --remove-orphans

"${cmd[@]}"