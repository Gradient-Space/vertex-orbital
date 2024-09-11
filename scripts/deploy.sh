#!/usr/bin/env bash
podman build -f Containerfile --rm -t vertex-orbital:v0.1 --no-cache

source .env
export PGPASSWORD="${DB_PASS}" && \
until pg_isready -h ${DB_HOST} -U ${DB_USER} -p ${DB_PORT} -d ${DB_NAME}; do
    >&2 echo "Postgres is not available yet - sleeping"
    sleep 1
done && \
podman run -dit --rm \
    --pod Vertex \
    --name vertex-orbital \
    -e DB_URL=${DB_URL} \
    -e DB_CHANNEL=${DB_CHANNEL} \
    -e PERIOD=${PERIOD} \
    localhost/vertex-orbital:v0.1
