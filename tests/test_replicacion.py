"""Replicación (síncrona/asíncrona), failover y decisiones CAP, contra el clúster real."""
import time
from uuid import uuid4

import psycopg
import pytest
from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture(scope="module")
def client():
    with TestClient(app) as c:
        c.post("/replicacion/modo", json={"modo": "async"})
        yield c
        c.post("/replicacion/modo", json={"modo": "async"})
        c.post("/nodos/restaurar")


@pytest.fixture(autouse=True)
def limpio(client):
    client.post("/nodos/restaurar")
    yield
    client.post("/nodos/restaurar")


def cluster():
    return app.state.cluster


def nuevo(client):
    r = client.post("/clientes", json={"nombre": "R", "email": f"{uuid4()}@x.com", "ciudad": "Cali"})
    assert r.status_code == 201, r.text
    return r.json()


def en_replica(shard, cid):
    with cluster().shards[shard].replica.connection() as c:
        return c.execute("SELECT 1 FROM clientes_core WHERE id=%s", (cid,)).fetchone() is not None


def esperar_replica(shard, cid, timeout=10):
    t = time.time()
    while time.time() - t < timeout:
        if en_replica(shard, cid):
            return True
        time.sleep(0.1)
    return False


def test_las_replicas_son_de_solo_lectura_y_estan_en_recuperacion():
    for nodo in cluster().nodos:
        with nodo.replica.connection() as c:
            assert c.execute("SELECT pg_is_in_recovery() AS r").fetchone()["r"] is True
        with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
            with nodo.replica.connection() as c:
                c.execute("CREATE TABLE no_debe_existir (x int)")


def test_replicacion_asincrona_consistencia_eventual(client):
    client.post("/replicacion/modo", json={"modo": "async"})
    c = nuevo(client)
    assert not en_replica(c["shard"], c["id"])       # el primario ya confirmó, la réplica aún no (lag)
    assert esperar_replica(c["shard"], c["id"])        # ...pero converge: consistencia eventual


def test_replicacion_sincrona_consistencia_fuerte(client):
    client.post("/replicacion/modo", json={"modo": "sync"})
    assert client.get("/topologia").json()["modo"] == "sync"
    t = time.time()
    c = nuevo(client)
    assert en_replica(c["shard"], c["id"])             # al responder, la réplica ya lo tiene
    assert time.time() - t > 0.5                        # costo: mayor latencia de escritura


def test_demo_compara_ambos_modos(client):
    client.post("/replicacion/modo", json={"modo": "async"})
    a = client.post("/replicacion/demo").json()
    client.post("/replicacion/modo", json={"modo": "sync"})
    s = client.post("/replicacion/demo").json()
    assert a["visible_en_replica_inmediato"] is False and a["retraso_replica_ms"] > 100
    assert s["visible_en_replica_inmediato"] is True
    assert s["escritura_ms"] > a["escritura_ms"]


def test_failover_lectura_primario_caido_AP(client):
    c = nuevo(client)
    assert esperar_replica(c["shard"], c["id"])
    client.post(f"/nodos/shard{c['shard']}/primary?caido=true")
    r = client.get(f"/clientes/{c['id']}")
    assert r.status_code == 200
    assert r.json()["_nodos_consultados"] == [f"shard{c['shard']}-replica"]   # respondió la réplica
    assert r.json()["_posiblemente_desactualizado"] is True                  # disponibilidad > consistencia


def test_escritura_con_primario_caido_se_rechaza_CP(client):
    client.post("/nodos/shard0/primary?caido=true")
    client.post("/nodos/shard1/primary?caido=true")
    client.post("/nodos/shard2/primary?caido=true")
    r = client.post("/clientes", json={"nombre": "R", "email": f"{uuid4()}@x.com", "ciudad": "Cali"})
    assert r.status_code == 503 and r.json()["teorema"] == "CP"


def test_escritura_sincrona_sin_replica_se_bloquea_CP(client):
    client.post("/replicacion/modo", json={"modo": "sync"})
    for i in range(3):
        client.post(f"/nodos/shard{i}/replica?caido=true")
    r = client.post("/clientes", json={"nombre": "R", "email": f"{uuid4()}@x.com", "ciudad": "Cali"})
    assert r.status_code == 503
    client.post("/replicacion/modo", json={"modo": "async"})   # en asíncrono, sigue disponible (AP)
    assert client.post("/clientes", json={"nombre": "R", "email": f"{uuid4()}@x.com", "ciudad": "Cali"}).status_code == 201


def test_fallo_real_de_conexion_hace_failover(client):
    """Un primario inalcanzable de verdad (no simulado) también cae a la réplica."""
    nodo = cluster().shards[0]
    original = nodo.primary
    from app.db import _pool
    nodo.primary = _pool("postgresql://app:app@127.0.0.1:1/tienda")  # puerto sin servicio
    try:
        with nodo.read() as (conn, served):
            assert served == "shard0-replica"
    finally:
        nodo.primary.close()
        nodo.primary = original
