#!/bin/bash
# Arranca una réplica física (streaming replication) del primario $PRIMARY_HOST.
# REPLICA_DELAY_MS retrasa la aplicación de cambios para que el lag sea visible en la demo.
set -e
PGDATA=${PGDATA:-/var/lib/postgresql/data}
if [ ! -s "$PGDATA/PG_VERSION" ]; then
  until PGPASSWORD=app pg_isready -q -h "$PRIMARY_HOST" -U app -d tienda; do sleep 1; done
  mkdir -p "$PGDATA"; chown postgres:postgres "$PGDATA"; chmod 700 "$PGDATA"
  gosu postgres env PGPASSWORD=app pg_basebackup -h "$PRIMARY_HOST" -U app -D "$PGDATA" -R -X stream
  echo "recovery_min_apply_delay = '${REPLICA_DELAY_MS:-1000}ms'" >> "$PGDATA/postgresql.auto.conf"
fi
exec gosu postgres postgres
