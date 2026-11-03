#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
python3 deploy/ldap/ensure-settings-key.py
docker compose -f deploy/ldap/docker-compose.yml up --build -d --wait
docker compose --env-file deploy/ldap/.env -f docker-compose.yml -f deploy/docker-compose.auth.yml up --build -d api ui fluent-bit
