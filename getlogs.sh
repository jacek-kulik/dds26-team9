#!/usr/bin/env bash

set -e

TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")
OUT_DIR="benchmark_logs/$TIMESTAMP"

mkdir -p "$OUT_DIR"

echo "Collecting logs into $OUT_DIR"
echo "-------------------------------------"

# Detect docker compose command
if docker compose version >/dev/null 2>&1; then
    COMPOSE_CMD="docker compose"
else
    COMPOSE_CMD="docker-compose"
fi

# Get running container IDs
CONTAINERS=$($COMPOSE_CMD ps -q)

if [ -z "$CONTAINERS" ]; then
    echo "No running containers found."
    exit 1
fi

for CID in $CONTAINERS; do
    NAME=$(docker inspect --format='{{.Name}}' "$CID" | sed 's/\///')
    echo "→ Exporting logs for $NAME"

    docker logs "$CID" &> "$OUT_DIR/$NAME.log" || true
done

echo "-------------------------------------"
echo "Logs collected successfully."
echo "Location: $OUT_DIR"