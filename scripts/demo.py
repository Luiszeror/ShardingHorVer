"""Demostración por consola: python -m scripts.demo  (requiere el clúster arriba)."""
import time

from app.db import Cluster
from app.seed import seed

c = Cluster()
c.reset()
print("== Cargando 6000 clientes con sus pedidos ==")
seed(c, 6000)
s = c.stats()
print("\n== HORIZONTAL: distribución por shard ==")
for x in s["shards"]:
    print(f"  shard{x['shard']}: {x['clientes']:>5} clientes {x['pedidos']:>5} pedidos  ventas={x['ventas']:>12}  {x['tamano']}")
print("  TOTAL:", s["total"])

print("\n== VERTICAL: columnas separadas ==")
print(f"  nodo perfil: {s['perfil']['perfiles']} perfiles, {s['perfil']['tamano']} (bio/avatar/preferencias)")
with c.shards[0].connection() as k:
    print("  clientes_core (shard0) pesa:", k.execute("SELECT pg_size_pretty(pg_total_relation_size('clientes_core')) AS s").fetchone()["s"])
with c.perfil.connection() as k:
    print("  clientes_perfil pesa:       ", k.execute("SELECT pg_size_pretty(pg_total_relation_size('clientes_perfil')) AS s").fetchone()["s"])

print("\n== ENRUTAMIENTO: lookup por id vs scatter-gather por ciudad ==")
with c.shards[1].connection() as k:
    cid = k.execute("SELECT id FROM clientes_core LIMIT 1").fetchone()["id"]
def t(f, n=200):
    a = time.perf_counter()
    for _ in range(n): f()
    return (time.perf_counter() - a) / n * 1000
r = c.obtener_cliente(cid)
print(f"  id {cid} -> nodos: {r['_nodos_consultados']}  ({t(lambda: c.obtener_cliente(cid)):.2f} ms)")
r = c.obtener_cliente(cid, True)
print(f"  con perfil -> nodos: {r['_nodos_consultados']}  ({t(lambda: c.obtener_cliente(cid, True)):.2f} ms)")
r = c.buscar_por_ciudad("Cali")
print(f"  ciudad=Cali -> nodos: {r['_nodos_consultados']}  ({t(lambda: c.buscar_por_ciudad('Cali'), 50):.2f} ms)")
c.close()
