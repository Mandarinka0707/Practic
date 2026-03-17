#!/bin/sh
set -eu

CRON_SCHEDULE="${ETL_CRON:-*/1 * * * *}"

# Cron does NOT automatically inherit docker container env, so we explicitly
# write required variables into the cron file.
cat > /etc/cron.d/etl-cron <<EOF
SHELL=/bin/sh
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

POSTGRES_HOST=${POSTGRES_HOST}
POSTGRES_PORT=${POSTGRES_PORT}
POSTGRES_DB=${POSTGRES_DB}
POSTGRES_USER=${POSTGRES_USER}
POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
MONGO_HOST=${MONGO_HOST}
MONGO_PORT=${MONGO_PORT}
MONGO_DB=${MONGO_DB}
LOG_LEVEL=${LOG_LEVEL:-INFO}
SYNC_WINDOW_LAG_SECONDS=${SYNC_WINDOW_LAG_SECONDS:-5}
STATE_DOC_ID=${STATE_DOC_ID:-replication_state}

${CRON_SCHEDULE} root python /app/replicate.py >> /proc/1/fd/1 2>> /proc/1/fd/2
EOF

chmod 0644 /etc/cron.d/etl-cron

echo "[etl] Cron schedule: ${CRON_SCHEDULE}"
echo "[etl] Starting cron in foreground..."

cron -f

