from contextlib import contextmanager
from uuid import UUID, uuid4

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from . import config
from .router import shard_index


class Cluster:
    """Fachada sobre los nodos: N shards horizontales + 1 nodo vertical (perfil)."""

    def __init__(self, shard_dsns=None, perfil_dsn=None):
        self.shard_dsns = shard_dsns or config.SHARD_DSNS
        self.shards = [
            ConnectionPool(d, min_size=1, max_size=5, kwargs={"row_factory": dict_row}, open=True)
            for d in self.shard_dsns
        ]
        self.perfil = ConnectionPool(
            perfil_dsn or config.PERFIL_DSN, min_size=1, max_size=5,
            kwargs={"row_factory": dict_row}, open=True,
        )

    @property
    def n(self) -> int:
        return len(self.shards)

    def route(self, cliente_id) -> int:
        return shard_index(cliente_id, self.n)

    @contextmanager
    def shard_for(self, cliente_id):
        with self.shards[self.route(cliente_id)].connection() as c:
            yield c

    # ---------- Clientes (horizontal + vertical) ----------
    def crear_cliente(self, nombre, email, ciudad, bio=None, preferencias=None, avatar=None) -> dict:
        cid = uuid4()
        idx = self.route(cid)
        # 1) fragmento core -> shard horizontal elegido por hash(cid)
        with self.shards[idx].connection() as c:
            c.execute(
                "INSERT INTO clientes_core (id, nombre, email, ciudad) VALUES (%s,%s,%s,%s)",
                (cid, nombre, email, ciudad),
            )
        # 2) fragmento vertical -> nodo perfil. Si falla, compensamos (saga simple).
        try:
            with self.perfil.connection() as c:
                c.execute(
                    "INSERT INTO clientes_perfil (cliente_id, bio, avatar, preferencias) VALUES (%s,%s,%s,%s)",
                    (cid, bio, avatar, Jsonb(preferencias or {})),
                )
        except Exception:
            with self.shards[idx].connection() as c:
                c.execute("DELETE FROM clientes_core WHERE id=%s", (cid,))
            raise
        return {"id": str(cid), "shard": idx}

    def obtener_cliente(self, cid: UUID, con_perfil=False) -> dict | None:
        idx = self.route(cid)
        with self.shards[idx].connection() as c:
            row = c.execute("SELECT * FROM clientes_core WHERE id=%s", (cid,)).fetchone()
        if not row:
            return None
        row["_shard"] = idx
        row["_nodos_consultados"] = [f"shard{idx}"]
        if con_perfil:  # solo se toca el nodo vertical si se pide
            with self.perfil.connection() as c:
                p = c.execute(
                    "SELECT bio, preferencias, length(avatar) AS avatar_bytes FROM clientes_perfil WHERE cliente_id=%s",
                    (cid,),
                ).fetchone()
            row["perfil"] = p
            row["_nodos_consultados"].append("perfil")
        return row

    def eliminar_cliente(self, cid: UUID) -> bool:
        with self.shard_for(cid) as c:
            n = c.execute("DELETE FROM clientes_core WHERE id=%s", (cid,)).rowcount
        with self.perfil.connection() as c:
            c.execute("DELETE FROM clientes_perfil WHERE cliente_id=%s", (cid,))
        return n > 0

    def buscar_por_ciudad(self, ciudad: str, limit=20) -> dict:
        """Consulta SIN shard key -> scatter-gather a todos los shards."""
        out = []
        for i, pool in enumerate(self.shards):
            with pool.connection() as c:
                for r in c.execute(
                    "SELECT id, nombre, email, ciudad FROM clientes_core WHERE ciudad=%s ORDER BY nombre LIMIT %s",
                    (ciudad, limit),
                ):
                    r["_shard"] = i
                    out.append(r)
        out.sort(key=lambda r: r["nombre"])
        return {"resultados": out[:limit], "_nodos_consultados": [f"shard{i}" for i in range(self.n)]}

    def buscar_por_email(self, email: str) -> dict | None:
        """Sin shard key: se pregunta a todos (en producción se usaría una tabla índice/lookup)."""
        for i, pool in enumerate(self.shards):
            with pool.connection() as c:
                r = c.execute("SELECT * FROM clientes_core WHERE email=%s", (email,)).fetchone()
            if r:
                r["_shard"] = i
                return r
        return None

    # ---------- Pedidos (co-ubicados con el cliente) ----------
    def crear_pedido(self, cid: UUID, items: list[dict]) -> dict:
        idx = self.route(cid)
        with self.shards[idx].connection() as c:
            with c.transaction():  # transacción LOCAL: todo vive en un solo shard
                if not c.execute("SELECT 1 FROM clientes_core WHERE id=%s", (cid,)).fetchone():
                    raise LookupError("cliente no existe")
                pid, total, filas = uuid4(), 0, []
                for it in items:
                    prod = c.execute("SELECT precio FROM productos WHERE id=%s", (it["producto_id"],)).fetchone()
                    if not prod:
                        raise LookupError(f"producto {it['producto_id']} no existe")
                    total += prod["precio"] * it["cantidad"]
                    filas.append((pid, it["producto_id"], it["cantidad"], prod["precio"]))
                c.execute("INSERT INTO pedidos (id, cliente_id, total) VALUES (%s,%s,%s)", (pid, cid, total))
                for f in filas:
                    c.execute(
                        "INSERT INTO pedido_items (pedido_id, producto_id, cantidad, precio) VALUES (%s,%s,%s,%s)", f
                    )
        return {"id": str(pid), "total": str(total), "shard": idx}

    def pedidos_de(self, cid: UUID) -> dict:
        idx = self.route(cid)
        with self.shards[idx].connection() as c:
            rows = c.execute(
                "SELECT id, total, creado_en FROM pedidos WHERE cliente_id=%s ORDER BY creado_en DESC", (cid,)
            ).fetchall()
        return {"pedidos": rows, "_nodos_consultados": [f"shard{idx}"]}

    def productos(self) -> list[dict]:
        # tabla replicada: cualquier shard sirve
        with self.shards[0].connection() as c:
            return c.execute("SELECT * FROM productos ORDER BY id").fetchall()

    # ---------- Estadísticas (scatter-gather + agregación) ----------
    def stats(self) -> dict:
        shards, tot_c, tot_p, tot_v = [], 0, 0, 0
        for i, pool in enumerate(self.shards):
            with pool.connection() as c:
                r = c.execute(
                    "SELECT (SELECT count(*) FROM clientes_core) AS clientes,"
                    " (SELECT count(*) FROM pedidos) AS pedidos,"
                    " (SELECT coalesce(sum(total),0) FROM pedidos) AS ventas,"
                    " pg_size_pretty(pg_database_size(current_database())) AS tamano"
                ).fetchone()
            shards.append({"shard": i, **r, "ventas": str(r["ventas"])})
            tot_c += r["clientes"]; tot_p += r["pedidos"]; tot_v += r["ventas"]
        with self.perfil.connection() as c:
            pr = c.execute(
                "SELECT count(*) AS perfiles, pg_size_pretty(pg_database_size(current_database())) AS tamano FROM clientes_perfil"
            ).fetchone()
        return {
            "shards": shards,
            "perfil": pr,
            "total": {"clientes": tot_c, "pedidos": tot_p, "ventas": str(tot_v)},
        }

    def reset(self):
        for pool in self.shards:
            with pool.connection() as c:
                c.execute("TRUNCATE clientes_core CASCADE")
        with self.perfil.connection() as c:
            c.execute("TRUNCATE clientes_perfil")

    def close(self):
        for p in [*self.shards, self.perfil]:
            p.close()
