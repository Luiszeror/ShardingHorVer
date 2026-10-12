"""Validación del sharding contra un clúster real (ver scripts/local_cluster.sh o docker compose)."""
from collections import Counter
from decimal import Decimal
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.router import shard_index
from app.seed import seed


@pytest.fixture(scope="module")
def client():
    with TestClient(app) as c:
        app.state.cluster.reset()
        seed(app.state.cluster, 1500)
        yield c


def cluster():
    return app.state.cluster


def test_router_determinista_y_uniforme():
    ids = [uuid4() for _ in range(30000)]
    assert all(shard_index(i, 3) == shard_index(i, 3) for i in ids[:100])
    dist = Counter(shard_index(i, 3) for i in ids)
    for n in dist.values():
        assert abs(n - 10000) < 400  # ±4 %


def test_horizontal_cada_fila_esta_en_su_shard_y_solo_en_ese(client):
    """Se consulta CADA nodo directamente (saltándose la API) para comprobar la ubicación física."""
    total = 0
    for i, pool in enumerate(cluster().shards):
        with pool.connection() as c:
            ids = [r["id"] for r in c.execute("SELECT id FROM clientes_core")]
        assert ids, f"shard{i} vacío"
        assert all(cluster().route(x) == i for x in ids)
        total += len(ids)
    assert total == 1500


def test_horizontal_pedidos_colocados_con_su_cliente(client):
    for i, pool in enumerate(cluster().shards):
        with pool.connection() as c:
            huerfanos = c.execute(
                "SELECT count(*) AS n FROM pedidos p LEFT JOIN clientes_core c ON c.id=p.cliente_id WHERE c.id IS NULL"
            ).fetchone()["n"]
            assert huerfanos == 0


def test_vertical_columnas_separadas_fisicamente(client):
    with cluster().shards[0].connection() as c:
        cols = {r["column_name"] for r in c.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name='clientes_core'")}
        assert not cols & {"bio", "avatar", "preferencias"}
        assert not c.execute("SELECT to_regclass('clientes_perfil') AS t").fetchone()["t"]
    with cluster().perfil.connection() as c:
        cols = {r["column_name"] for r in c.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name='clientes_perfil'")}
        assert {"bio", "avatar", "preferencias"} <= cols
        assert not c.execute("SELECT to_regclass('clientes_core') AS t").fetchone()["t"]
        assert c.execute("SELECT count(*) AS n FROM clientes_perfil").fetchone()["n"] == 1500


def test_api_crud_toca_solo_los_nodos_necesarios(client):
    r = client.post("/clientes", json={"nombre": "Prueba", "email": f"{uuid4()}@x.com", "ciudad": "Cali",
                                       "bio": "hola", "preferencias": {"tema": "oscuro"}})
    assert r.status_code == 201
    cid, shard = r.json()["id"], r.json()["shard"]
    assert client.get(f"/routing/{cid}").json()["shard"] == shard

    core = client.get(f"/clientes/{cid}").json()
    assert core["_nodos_consultados"] == [f"shard{shard}"]          # lectura caliente: 1 nodo
    full = client.get(f"/clientes/{cid}?perfil=true").json()
    assert full["_nodos_consultados"] == [f"shard{shard}", "perfil"]  # join vertical bajo demanda
    assert full["perfil"]["bio"] == "hola"

    p = client.post("/pedidos", json={"cliente_id": cid, "items": [{"producto_id": 1, "cantidad": 2},
                                                                   {"producto_id": 2, "cantidad": 1}]})
    assert p.status_code == 201 and Decimal(p.json()["total"]) == 300000
    ped = client.get(f"/clientes/{cid}/pedidos").json()
    assert ped["_nodos_consultados"] == [f"shard{shard}"] and len(ped["pedidos"]) == 1

    assert client.delete(f"/clientes/{cid}").status_code == 204
    assert client.get(f"/clientes/{cid}").status_code == 404
    with cluster().perfil.connection() as c:  # también se limpió el fragmento vertical
        assert c.execute("SELECT 1 FROM clientes_perfil WHERE cliente_id=%s", (cid,)).fetchone() is None


def test_scatter_gather_coincide_con_la_verdad(client):
    s = client.get("/stats").json()
    assert s["total"]["clientes"] == sum(x["clientes"] for x in s["shards"]) == 1500
    assert all(x["clientes"] > 300 for x in s["shards"])  # reparto razonable
    # Verdad de referencia: sumar a mano consultando cada nodo
    ventas = Decimal(0)
    for pool in cluster().shards:
        with pool.connection() as c:
            ventas += c.execute("SELECT coalesce(sum(total),0) AS v FROM pedidos").fetchone()["v"]
    assert Decimal(s["total"]["ventas"]) == ventas
    r = client.get("/clientes", params={"ciudad": "Bogotá", "limit": 5000}).json()
    esperado = 0
    for pool in cluster().shards:
        with pool.connection() as c:
            esperado += c.execute("SELECT count(*) AS n FROM clientes_core WHERE ciudad='Bogotá'").fetchone()["n"]
    assert len(r["resultados"]) == esperado and len(r["_nodos_consultados"]) == 3


def test_pedido_invalido_no_deja_basura(client):
    cid = client.post("/clientes", json={"nombre": "N", "email": f"{uuid4()}@x.com", "ciudad": "Cali"}).json()["id"]
    r = client.post("/pedidos", json={"cliente_id": cid, "items": [{"producto_id": 1, "cantidad": 1},
                                                                   {"producto_id": 999, "cantidad": 1}]})
    assert r.status_code == 404
    assert client.get(f"/clientes/{cid}/pedidos").json()["pedidos"] == []  # rollback local


def test_compensacion_si_falla_el_fragmento_vertical(client, monkeypatch):
    email = f"{uuid4()}@x.com"
    class Roto:
        def write(self): raise RuntimeError("nodo perfil caído")
    monkeypatch.setattr(cluster(), "perfil", Roto())
    with pytest.raises(RuntimeError):
        cluster().crear_cliente("X", email, "Cali")
    monkeypatch.undo()
    assert cluster().buscar_por_email(email) is None  # el core fue revertido
