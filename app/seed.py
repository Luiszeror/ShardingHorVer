"""Carga masiva: agrupa por shard y hace inserciones en lote (así se usa el router a escala)."""
import random
from uuid import uuid4

from psycopg.types.json import Jsonb

CIUDADES = ["Bogotá", "Medellín", "Cali", "Barranquilla", "Cartagena", "Bucaramanga"]
NOMBRES = ["Ana", "Luis", "María", "Carlos", "Sofía", "Juan", "Laura", "Pedro", "Valentina", "Andrés"]


def seed(cluster, n_clientes=3000, max_pedidos=4, seed_value=42):
    rnd = random.Random(seed_value)
    prods = {p["id"]: p["precio"] for p in cluster.productos()}
    por_shard = [dict(c=[], p=[], i=[]) for _ in range(cluster.n)]
    perfiles = []
    for k in range(n_clientes):
        cid = uuid4()
        b = por_shard[cluster.route(cid)]
        b["c"].append((cid, f"{rnd.choice(NOMBRES)} {k}", f"user{k}-{cid.hex[:6]}@mail.com", rnd.choice(CIUDADES)))
        perfiles.append((cid, f"Bio del cliente {k}. " + "lorem ipsum " * 20, Jsonb({"tema": rnd.choice(["claro", "oscuro"])})))
        for _ in range(rnd.randint(0, max_pedidos)):
            pid, total, items = uuid4(), 0, []
            for prod in rnd.sample(list(prods), rnd.randint(1, 3)):
                q = rnd.randint(1, 3)
                total += prods[prod] * q
                items.append((pid, prod, q, prods[prod]))
            b["p"].append((pid, cid, total))
            b["i"].extend(items)
    for pool, b in zip(cluster.shards, por_shard):
        with pool.connection() as c, c.cursor() as cur:
            cur.executemany("INSERT INTO clientes_core (id,nombre,email,ciudad) VALUES (%s,%s,%s,%s)", b["c"])
            cur.executemany("INSERT INTO pedidos (id,cliente_id,total) VALUES (%s,%s,%s)", b["p"])
            cur.executemany("INSERT INTO pedido_items (pedido_id,producto_id,cantidad,precio) VALUES (%s,%s,%s,%s)", b["i"])
    with cluster.perfil.connection() as c, c.cursor() as cur:
        cur.executemany("INSERT INTO clientes_perfil (cliente_id,bio,preferencias) VALUES (%s,%s,%s)", perfiles)
    return {"clientes": n_clientes}
