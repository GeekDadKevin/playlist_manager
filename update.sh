#!/bin/bash
# update.sh - Update repo, rebuild, and restart Docker Compose

echo "Pulling latest changes from git..."
git pull || { echo "git pull failed"; exit 1; }

echo "Building Docker Compose services..."
docker compose build || { echo "docker compose build failed"; exit 1; }

echo "Starting Docker Compose services in detached mode..."
docker compose up -d || { echo "docker compose up -d failed"; exit 1; }

echo "Update complete."
