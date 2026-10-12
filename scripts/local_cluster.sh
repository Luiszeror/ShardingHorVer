#!/usr/bin/env bash
# Levanta el mismo clúster SIN Docker: 4 instancias PostgreSQL locales (puertos 5441-5444).
# Uso: scripts/local_cluster.sh start|stop|status
set -euo pipefail
PGBIN=${PGBIN:-$(ls -d /usr/lib/postgresql/*/bin | sort -V | tail -1)}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
DATA=$ROOT/.pgdata
NODES=("shard0:5441:shard_init" "shard1:5442:shard_init" "shard2:5443:shard_init" "perfil:5444:perfil_init")
# PostgreSQL no corre como root: usamos el usuario 'postgres' si somos root.
run() { if [ "$(id -u)" = 0 ]; then su postgres -s /bin/bash -c "$*"; else bash -c "$*"; fi; }

case "${1:-start}" in
  start)
    mkdir -p "$DATA"; [ "$(id -u)" = 0 ] && chown postgres "$DATA" && chmod a+rx "$ROOT" "$ROOT/sql"
    for n in "${NODES[@]}"; do IFS=: read -r name port sql <<<"$n"
      d=$DATA/$name
      if [ ! -d "$d" ]; then run "$PGBIN/initdb -D $d -U app --auth=trust >/dev/null"; fi
      run "$PGBIN/pg_ctl -D $d -o '-p $port -k /tmp -c listen_addresses=127.0.0.1' -l $d.log -w start >/dev/null" || true
      run "$PGBIN/psql -h 127.0.0.1 -p $port -U app -d postgres -tc \"SELECT 1 FROM pg_database WHERE datname='tienda'\" | grep -q 1 || $PGBIN/createdb -h 127.0.0.1 -p $port -U app tienda"
      run "$PGBIN/psql -q -h 127.0.0.1 -p $port -U app -d tienda -f $ROOT/sql/$sql.sql >/dev/null"
      echo "OK $name :$port"
    done;;
  stop)
    for n in "${NODES[@]}"; do IFS=: read -r name _ _ <<<"$n"; run "$PGBIN/pg_ctl -D $DATA/$name -m fast stop" || true; done;;
  status)
    for n in "${NODES[@]}"; do IFS=: read -r name port _ <<<"$n"; "$PGBIN/pg_isready" -h 127.0.0.1 -p "$port" || true; done;;
esac
