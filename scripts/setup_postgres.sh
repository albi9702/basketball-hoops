#!/usr/bin/env bash
set -euo pipefail

# Creates (or starts) a local PostgreSQL instance via Docker and wires .env settings
# so the scraper can persist data to it. Customize via environment variables before
# running:
#   CONTAINER_NAME=basketball-hoops-db POSTGRES_PASSWORD=secret ./scripts/setup_postgres.sh

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONTAINER_NAME="${CONTAINER_NAME:-basketball-hoops-db}"
POSTGRES_USER="${POSTGRES_USER:-postgres}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-postgres}"
POSTGRES_DB="${POSTGRES_DB:-basketball_hoops}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_IMAGE="${POSTGRES_IMAGE:-postgres:16}"
SCHEMA_NAME="${DATABASE_SCHEMA:-basketball_hoops}"

install_docker_if_needed() {
  if command -v docker >/dev/null 2>&1; then
    return
  fi

  echo "Docker not found. Attempting to install..."
  if [[ -f /etc/debian_version ]]; then
    sudo apt-get update
    sudo apt-get install -y ca-certificates curl gnupg
    sudo install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    echo \
"deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
$(. /etc/os-release && echo $VERSION_CODENAME) stable" | \
      sudo tee /etc/apt/sources.list.d/docker.list >/dev/null
    sudo apt-get update
    sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
  elif [[ -f /etc/fedora-release ]]; then
    sudo dnf -y install dnf-plugins-core
    sudo dnf config-manager --add-repo https://download.docker.com/linux/fedora/docker-ce.repo
    sudo dnf -y install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
    sudo systemctl enable --now docker
  else
    echo "Unsupported OS for automatic Docker installation. Please install Docker manually." >&2
    exit 1
  fi
}

install_docker_if_needed

echo "Ensuring Postgres container '${CONTAINER_NAME}' is running (image ${POSTGRES_IMAGE})"
if ! docker ps -a --format '{{.Names}}' | grep -qx "${CONTAINER_NAME}"; then
  docker run -d \
    --name "${CONTAINER_NAME}" \
    -e "POSTGRES_USER=${POSTGRES_USER}" \
    -e "POSTGRES_PASSWORD=${POSTGRES_PASSWORD}" \
    -e "POSTGRES_DB=${POSTGRES_DB}" \
    -p "${POSTGRES_PORT}:5432" \
    ${POSTGRES_IMAGE} >/dev/null
else
  docker start "${CONTAINER_NAME}" >/dev/null
fi

echo "Waiting for PostgreSQL to accept connections..."
until docker exec "${CONTAINER_NAME}" pg_isready -U "${POSTGRES_USER}" >/dev/null 2>&1; do
  sleep 1
done

echo "Creating schema '${SCHEMA_NAME}' if it does not exist"
docker exec "${CONTAINER_NAME}" psql \
  -U "${POSTGRES_USER}" \
  -d "${POSTGRES_DB}" \
  -c "CREATE SCHEMA IF NOT EXISTS \"${SCHEMA_NAME}\";" >/dev/null

ENV_FILE="${ROOT_DIR}/.env"
touch "${ENV_FILE}"
update_env_var() {
  local key="$1"
  local value="$2"
  if grep -q "^${key}=" "${ENV_FILE}"; then
    sed -i "s|^${key}=.*$|${key}=${value}|" "${ENV_FILE}"
  else
    echo "${key}=${value}" >> "${ENV_FILE}"
  fi
}

DATABASE_URL="postgresql+psycopg://${POSTGRES_USER}:${POSTGRES_PASSWORD}@localhost:${POSTGRES_PORT}/${POSTGRES_DB}"
update_env_var "DATABASE_URL" "${DATABASE_URL}"
update_env_var "DATABASE_SCHEMA" "${SCHEMA_NAME}"
update_env_var "DATABASE_SEASON_TABLE" "seasons"
update_env_var "DATABASE_SCHEDULE_TABLE" "schedule_games"
update_env_var "DATABASE_BOXSCORE_TABLE" "boxscores"

cat <<EOF

PostgreSQL is ready!
Container name: ${CONTAINER_NAME}
Connection string: ${DATABASE_URL}
Schema: ${SCHEMA_NAME}
Configuration saved to ${ENV_FILE}

You can now run:  DATABASE_URL=${DATABASE_URL} python src/main.py
EOF
