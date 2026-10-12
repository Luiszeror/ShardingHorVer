#!/bin/bash
# Permite que las réplicas se conecten por streaming replication (solo entorno de laboratorio).
echo "host replication all all scram-sha-256" >> "$PGDATA/pg_hba.conf"
