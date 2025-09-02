#!/bin/bash

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "Stopping and removing any running containers for the project..."
cd "$PROJECT_DIR"
docker compose down --remove-orphans

echo "Removing any leftover containers with conflicting names..."
docker rm -f mqtt_broker store-primary store-backup1 store-backup2 monitor client1 client2 client3 client4 client5 application 2>/dev/null

echo "Building images (if needed)..."
docker compose build

echo "Starting all services..."
docker compose up -d

echo "All services started!"
docker compose ps
