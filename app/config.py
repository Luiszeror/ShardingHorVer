import os

# Lista de shards horizontales (el orden define el índice del shard).
SHARD_DSNS = [
    d.strip()
    for d in os.getenv(
        "SHARD_DSNS",
        ",".join(f"postgresql://app:app@127.0.0.1:{p}/tienda" for p in (5441, 5442, 5443)),
    ).split(",")
    if d.strip()
]
# Nodo del fragmento vertical (columnas pesadas/frías del cliente).
PERFIL_DSN = os.getenv("PERFIL_DSN", "postgresql://app:app@127.0.0.1:5444/tienda")
