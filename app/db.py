import time
from contextlib import ExitStack, contextmanager
from uuid import UUID, uuid4

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool, PoolTimeout

from . import config
from .router import shard_index


class NodoCaido(Exception):
    """Un nodo no está disponible; `teorema` explica la decisión CAP tomada."""

    def __init__(self, msg, teorema="CP"):
        super().__init__(msg)
        self.teorema = teorema


def _pool(dsn):
    return ConnectionPool(dsn, min_size=1, max_size=5, kwargs={"row_factory": dict_row}, open=True)


class Nodo:
    """Un nodo lógico = primario (lecturas/escrituras) + réplica opcional (solo lectura).

    Los flags `caido_*` simulan fallos para la demo CAP sin tocar los contenedores.
    """

    def __init__(self, nombre, dsn, replica_dsn, cluster):
        self.nombre, self.cluster = nombre, cluster
        self.dsn, self.replica_dsn = dsn, replica_dsn
        self.primary = _pool(dsn)
        self.replica = _pool(replica_dsn) if replica_dsn else None
        self.caido_primary = self.caido_replica = False

    def connection(self):
        """Conexión cruda al primario (administración, seed y tests; ignora la simulación)."""
        return self.primary.connection()

    @contextmanager
    def write(self):
        if self.caido_primary:
            raise NodoCaido(f"{self.nombre} caído: escritura rechazada (se prioriza consistencia → CP)")
        if self.cluster.modo == "sync" and self.replica and self.caido_replica:
            raise NodoCaido(
                f"{self.nombre}: sin réplica síncrona, el commit queda bloqueado (se prioriza consistencia → CP)"
            )
        with self.primary.connection() as c:
            yield c

    @contextmanager
    def read(self, replica=False):
        """Rinde (conexión, nodo_que_respondió). Si el primario falla, hace failover a la réplica (AP)."""
        with ExitStack() as st:
            usar_replica = replica or self.caido_primary
            served = None
            if not usar_replica:
                try:
                    conn = st.enter_context(self.primary.connection(timeout=2))
                    served = self.nombre
                except (psycopg.OperationalError, PoolTimeout):
                    usar_replica = True
            if usar_replica:
                if not self.replica or self.caido_replica:
                    raise NodoCaido(f"{self.nombre}: ni primario ni réplica disponibles", "CP")
                conn = st.enter_context(self.replica.connection(timeout=2))
                served = f"{self.nombre}-replica"
            yield conn, served

    def close(self):
        self.primary.close()
        if self.replica:
            self.replica.close()


def _desactualizado(served):
    return any(s.endswith("-replica") for s in served)


class Cluster:
    """Fachada: N shards horizontales + 1 nodo vertical (perfil), cada uno con réplica."""

    def __init__(self, shard_dsns=None, perfil_dsn=None, shard_replicas=None, perfil_replica=None):
        dsns = shard_dsns or config.SHARD_DSNS
        reps = config.SHARD_REPLICA_DSNS if shard_replicas is None else shard_replicas
        reps = list(reps) + [None] * (len(dsns) - len(reps))
        self.modo = "async"
        self.shards = [Nodo(f"shard{i}", d, reps[i] or None, self) for i, d in enumerate(dsns)]
        self.perfil = Nodo("perfil", perfil_dsn or config.PERFIL_DSN,
                           (perfil_replica if perfil_replica is not None else config.PERFIL_REPLICA_DSN) or None, self)
        try:
            self.modo = self._leer_modo()
        except Exception:
            pass

    @property
    def n(self) -> int:
        return len(self.shards)

    @property
    def nodos(self):
        return [*self.shards, self.perfil]

    def route(self, cliente_id) -> int:
        return shard_index(cliente_id, self.n)

    def nodo(self, nombre: str) -> Nodo:
        for n in self.nodos:
            if n.nombre == nombre:
                return n
        raise KeyError(nombre)

    # ---------- Clientes (horizontal + vertical) ----------
    def crear_cliente(self, nombre, email, ciudad, bio=None, preferencias=None, avatar=None) -> dict:
        cid = uuid4()
        idx = self.route(cid)
        nodo = self.shards[idx]
        # 1) fragmento core -> shard horizontal elegido por hash(cid)
        with nodo.write() as c:
            c.execute(
                "INSERT INTO clientes_core (id, nombre, email, ciudad) VALUES (%s,%s,%s,%s)",
                (cid, nombre, email, ciudad),
            )
        # 2) fragmento vertical -> nodo perfil. Si falla, compensamos (saga simple).
        try:
            with self.perfil.write() as c:
                c.execute(
                    "INSERT INTO clientes_perfil (cliente_id, bio, avatar, preferencias) VALUES (%s,%s,%s,%s)",
                    (cid, bio, avatar, Jsonb(preferencias or {})),
                )
        except Exception:
            with nodo.connection() as c:
                c.execute("DELETE FROM clientes_core WHERE id=%s", (cid,))
            raise
        return {"id": str(cid), "shard": idx}

    def obtener_cliente(self, cid: UUID, con_perfil=False, replica=False) -> dict | None:
        idx = self.route(cid)
        with self.shards[idx].read(replica) as (c, srv):
            row = c.execute("SELECT * FROM clientes_core WHERE id=%s", (cid,)).fetchone()
        if not row:
            return None
        row["_shard"] = idx
        row["_nodos_consultados"] = [srv]
        if con_perfil:  # solo se toca el nodo vertical si se pide
            with self.perfil.read(replica) as (c, srv2):
                row["perfil"] = c.execute(
                    "SELECT bio, preferencias, length(avatar) AS avatar_bytes FROM clientes_perfil WHERE cliente_id=%s",
                    (cid,),
                ).fetchone()
            row["_nodos_consultados"].append(srv2)
        row["_posiblemente_desactualizado"] = _desactualizado(row["_nodos_consultados"])
        return row

    def eliminar_cliente(self, cid: UUID) -> bool:
        with self.shards[self.route(cid)].write() as c:
            n = c.execute("DELETE FROM clientes_core WHERE id=%s", (cid,)).rowcount
        with self.perfil.write() as c:
            c.execute("DELETE FROM clientes_perfil WHERE cliente_id=%s", (cid,))
        return n > 0

    def buscar_por_ciudad(self, ciudad: str, limit=20, replica=False) -> dict:
        """Consulta SIN shard key -> scatter-gather a todos los shards."""
        out, usados = [], []
        for i, nodo in enumerate(self.shards):
            with nodo.read(replica) as (c, srv):
                usados.append(srv)
                for r in c.execute(
                    "SELECT id, nombre, email, ciudad FROM clientes_core WHERE ciudad=%s ORDER BY nombre LIMIT %s",
                    (ciudad, limit),
                ):
                    r["_shard"] = i
                    out.append(r)
        out.sort(key=lambda r: r["nombre"])
        return {"resultados": out[:limit], "total_encontrados": len(out), "_nodos_consultados": usados,
                "_posiblemente_desactualizado": _desactualizado(usados)}

    def buscar_por_email(self, email: str) -> dict | None:
        """Sin shard key: se pregunta a todos (en producción se usaría una tabla índice/lookup)."""
        for i, nodo in enumerate(self.shards):
            with nodo.read() as (c, _):
                r = c.execute("SELECT * FROM clientes_core WHERE email=%s", (email,)).fetchone()
            if r:
                r["_shard"] = i
                return r
        return None

    # ---------- Pedidos (co-ubicados con el cliente) ----------
    def crear_pedido(self, cid: UUID, items: list[dict]) -> dict:
        idx = self.route(cid)
        with self.shards[idx].write() as c:
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

    def pedidos_de(self, cid: UUID, replica=False) -> dict:
        with self.shards[self.route(cid)].read(replica) as (c, srv):
            rows = c.execute(
                "SELECT id, total, creado_en FROM pedidos WHERE cliente_id=%s ORDER BY creado_en DESC", (cid,)
            ).fetchall()
        return {"pedidos": rows, "_nodos_consultados": [srv], "_posiblemente_desactualizado": _desactualizado([srv])}

    def productos(self) -> list[dict]:
        # tabla replicada entre shards: cualquier shard sirve
        with self.shards[0].connection() as c:
            return c.execute("SELECT * FROM productos ORDER BY id").fetchall()

    # ---------- Estadísticas (scatter-gather + agregación) ----------
    def stats(self) -> dict:
        shards, tot_c, tot_p, tot_v, usados = [], 0, 0, 0, []
        for i, nodo in enumerate(self.shards):
            with nodo.read() as (c, srv):
                r = c.execute(
                    "SELECT (SELECT count(*) FROM clientes_core) AS clientes,"
                    " (SELECT count(*) FROM pedidos) AS pedidos,"
                    " (SELECT coalesce(sum(total),0) FROM pedidos) AS ventas,"
                    " pg_size_pretty(pg_database_size(current_database())) AS tamano"
                ).fetchone()
            usados.append(srv)
            shards.append({"shard": i, **r, "ventas": str(r["ventas"]), "servido_por": srv})
            tot_c += r["clientes"]; tot_p += r["pedidos"]; tot_v += r["ventas"]
        with self.perfil.read() as (c, srv):
            pr = c.execute(
                "SELECT count(*) AS perfiles, pg_size_pretty(pg_database_size(current_database())) AS tamano FROM clientes_perfil"
            ).fetchone()
        pr["servido_por"] = srv
        return {
            "shards": shards,
            "perfil": pr,
            "total": {"clientes": tot_c, "pedidos": tot_p, "ventas": str(tot_v)},
        }

    def reset(self):
        for nodo in self.shards:
            with nodo.connection() as c:
                c.execute("TRUNCATE clientes_core CASCADE")
        with self.perfil.connection() as c:
            c.execute("TRUNCATE clientes_perfil")

    # ---------- Replicación y fallos ----------
    def _leer_modo(self) -> str:
        with self.shards[0].connection() as c:
            v = c.execute("SHOW synchronous_standby_names").fetchone()["synchronous_standby_names"]
        return "sync" if v.strip() else "async"

    def set_modo(self, modo: str):
        """Cambia entre replicación síncrona (commit espera a la réplica) y asíncrona, en todos los primarios."""
        if modo not in ("sync", "async"):
            raise ValueError("modo debe ser 'sync' o 'async'")
        for nodo in self.nodos:
            if not nodo.replica:
                continue
            with psycopg.connect(nodo.dsn, autocommit=True) as c:  # ALTER SYSTEM no admite transacciones
                if modo == "sync":
                    c.execute("ALTER SYSTEM SET synchronous_standby_names = '*'")
                    c.execute("ALTER SYSTEM SET synchronous_commit = 'remote_apply'")
                else:
                    c.execute("ALTER SYSTEM RESET synchronous_standby_names")
                    c.execute("ALTER SYSTEM RESET synchronous_commit")
                c.execute("SELECT pg_reload_conf()")
        self.modo = modo

    def set_caido(self, nombre: str, rol: str, caido: bool):
        nodo = self.nodo(nombre)
        if rol == "primary":
            nodo.caido_primary = caido
        elif rol == "replica":
            nodo.caido_replica = caido
        else:
            raise ValueError("rol debe ser primary o replica")

    def restaurar_nodos(self):
        for n in self.nodos:
            n.caido_primary = n.caido_replica = False

    def topologia(self) -> dict:
        def filas(pool, tabla):
            try:
                with pool.connection(timeout=2) as c:
                    return c.execute(f"SELECT count(*) AS n FROM {tabla}").fetchone()["n"]
            except Exception:
                return None

        def sync_state(nodo):
            try:
                with nodo.primary.connection(timeout=2) as c:
                    r = c.execute("SELECT state, sync_state FROM pg_stat_replication LIMIT 1").fetchone()
                return r
            except Exception:
                return None

        nodos = []
        for nodo in self.nodos:
            tabla = "clientes_core" if nodo.nombre.startswith("shard") else "clientes_perfil"
            nodos.append({
                "nombre": nodo.nombre,
                "tipo": "horizontal" if nodo.nombre.startswith("shard") else "vertical",
                "primario": {"caido": nodo.caido_primary, "filas": filas(nodo.primary, tabla)},
                "replica": None if not nodo.replica else {
                    "caido": nodo.caido_replica, "filas": filas(nodo.replica, tabla),
                    "estado": sync_state(nodo),
                },
            })
        return {"modo": self.modo, "nodos": nodos}

    def demo_replicacion(self) -> dict:
        """Escribe un cliente y mide: latencia del commit y cuándo se ve en la réplica."""
        t0 = time.perf_counter()
        r = self.crear_cliente("Demo réplica", f"rep-{uuid4().hex[:8]}@demo.com", "Demo")
        escritura_ms = (time.perf_counter() - t0) * 1000
        nodo = self.shards[r["shard"]]
        if not nodo.replica or nodo.caido_replica:
            raise NodoCaido(f"{nodo.nombre}-replica no disponible", "AP")
        cid = UUID(r["id"])
        t1 = time.perf_counter()
        visible_inmediato, retraso_ms = False, None
        while time.perf_counter() - t1 < 15:
            with nodo.replica.connection() as c:
                ok = c.execute("SELECT 1 FROM clientes_core WHERE id=%s", (cid,)).fetchone()
            if ok:
                retraso_ms = (time.perf_counter() - t1) * 1000
                visible_inmediato = retraso_ms < 50
                break
            time.sleep(0.05)
        self.eliminar_cliente(cid)
        return {
            "modo": self.modo, "shard": r["shard"],
            "escritura_ms": round(escritura_ms), "visible_en_replica_inmediato": visible_inmediato,
            "retraso_replica_ms": None if retraso_ms is None else round(retraso_ms),
        }

    def close(self):
        for n in self.nodos:
            n.close()
