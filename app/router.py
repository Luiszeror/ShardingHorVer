"""Enrutamiento del sharding HORIZONTAL.

Shard key: ``cliente_id`` (UUID). Estrategia: hash estable (MD5) módulo N.
 - Distribución uniforme sin hot-spots por orden de inserción.
 - Todos los datos de un cliente (core + pedidos + items) quedan en el MISMO shard,
   así que sus consultas típicas tocan un solo nodo (sin joins distribuidos).
"""
import hashlib
from uuid import UUID


def shard_index(cliente_id: UUID | str, n_shards: int) -> int:
    # hash() de Python no es estable entre procesos; usamos MD5 para que el mapeo sea determinista.
    digest = hashlib.md5(str(cliente_id).encode()).digest()
    return int.from_bytes(digest[:8], "big") % n_shards
