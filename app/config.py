import os


def _list(name, default):
    return [d.strip() for d in os.getenv(name, default).split(",") if d.strip()]


def _dsn(port):
    return f"postgresql://app:app@127.0.0.1:{port}/tienda"


# Primarios de los shards horizontales (el orden define el índice del shard).
SHARD_DSNS = _list("SHARD_DSNS", ",".join(_dsn(p) for p in (5441, 5442, 5443)))
# Réplicas de lectura (mismo orden). Vacío => sin réplicas.
SHARD_REPLICA_DSNS = _list("SHARD_REPLICA_DSNS", ",".join(_dsn(p) for p in (5451, 5452, 5453)))
# Nodo del fragmento vertical (columnas pesadas/frías del cliente) y su réplica.
PERFIL_DSN = os.getenv("PERFIL_DSN", _dsn(5444))
PERFIL_REPLICA_DSN = os.getenv("PERFIL_REPLICA_DSN", _dsn(5454))
