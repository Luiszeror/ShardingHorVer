from contextlib import asynccontextmanager
from pathlib import Path
from uuid import UUID

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field

from .db import Cluster, NodoCaido
from .seed import seed


@asynccontextmanager
async def lifespan(app):
    app.state.cluster = Cluster()
    yield
    app.state.cluster.close()


app = FastAPI(title="Sharding horizontal y vertical con PostgreSQL", lifespan=lifespan)
cl = lambda: app.state.cluster  # noqa: E731


@app.exception_handler(NodoCaido)
def nodo_caido(_, e: NodoCaido):
    return JSONResponse(status_code=503, content={"error": "nodo_no_disponible", "detalle": str(e), "teorema": e.teorema})


class ClienteIn(BaseModel):
    nombre: str
    email: str
    ciudad: str
    bio: str | None = None
    preferencias: dict = {}


class ItemIn(BaseModel):
    producto_id: int
    cantidad: int = Field(gt=0)


class PedidoIn(BaseModel):
    cliente_id: UUID
    items: list[ItemIn] = Field(min_length=1)


@app.get("/")
def home():
    return FileResponse(Path(__file__).parent / "static" / "index.html")


@app.post("/clientes", status_code=201)
def crear_cliente(c: ClienteIn):
    try:
        return cl().crear_cliente(c.nombre, c.email, c.ciudad, c.bio, c.preferencias)
    except NodoCaido:
        raise
    except Exception as e:
        raise HTTPException(409, str(e).splitlines()[0])


@app.get("/clientes/{cid}")
def get_cliente(cid: UUID, perfil: bool = False, lectura: str = "primary"):
    r = cl().obtener_cliente(cid, perfil, lectura == "replica")
    if not r:
        raise HTTPException(404, "cliente no encontrado")
    return r


@app.delete("/clientes/{cid}", status_code=204)
def del_cliente(cid: UUID):
    if not cl().eliminar_cliente(cid):
        raise HTTPException(404, "cliente no encontrado")


@app.get("/clientes")
def buscar(ciudad: str | None = None, email: str | None = None, limit: int = 20, lectura: str = "primary"):
    if email:
        return {"resultado": cl().buscar_por_email(email)}
    if ciudad:
        return cl().buscar_por_ciudad(ciudad, limit, lectura == "replica")
    raise HTTPException(400, "indique ciudad o email")


@app.post("/pedidos", status_code=201)
def crear_pedido(p: PedidoIn):
    try:
        return cl().crear_pedido(p.cliente_id, [i.model_dump() for i in p.items])
    except LookupError as e:
        raise HTTPException(404, str(e))


@app.get("/clientes/{cid}/pedidos")
def pedidos(cid: UUID, lectura: str = "primary"):
    return cl().pedidos_de(cid, lectura == "replica")


@app.get("/productos")
def productos():
    return cl().productos()


@app.get("/stats")
def stats():
    return cl().stats()


@app.get("/routing/{cid}")
def routing(cid: UUID):
    return {"cliente_id": str(cid), "shard": cl().route(cid), "n_shards": cl().n}


@app.post("/demo/seed")
def demo_seed(n: int = 3000, reset: bool = False):
    if reset:
        cl().reset()
    return seed(cl(), n)


# ---------- Replicación y simulación de fallos (demo CAP) ----------
@app.get("/topologia")
def topologia():
    return cl().topologia()


class ModoIn(BaseModel):
    modo: str


@app.post("/replicacion/modo")
def set_modo(m: ModoIn):
    try:
        cl().set_modo(m.modo)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {"modo": cl().modo}


@app.post("/replicacion/demo")
def replicacion_demo():
    return cl().demo_replicacion()


@app.post("/nodos/{nombre}/{rol}")
def nodo_estado(nombre: str, rol: str, caido: bool = True):
    try:
        cl().set_caido(nombre, rol, caido)
    except (KeyError, ValueError):
        raise HTTPException(404, "nodo o rol inválido")
    return {"nombre": nombre, "rol": rol, "caido": caido}


@app.post("/nodos/restaurar")
def restaurar():
    cl().restaurar_nodos()
    return {"ok": True}
