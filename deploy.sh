#!/bin/bash
set -e

cd "$(dirname "$0")"

echo "=== zapchasti-parser deploy ==="

if [[ "$1" == "--build" ]]; then
    echo "Building and starting..."
    docker compose -f docker/docker-compose.prod.yml build
    docker compose -f docker/docker-compose.prod.yml up -d
else
    echo "Starting (no rebuild)..."
    docker compose -f docker/docker-compose.prod.yml up -d
fi

echo "=== Status ==="
docker compose -f docker/docker-compose.prod.yml ps

echo "=== Done ==="
