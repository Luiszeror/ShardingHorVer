# Sharding horizontal y vertical con PostgreSQL (+ replicación y CAP)

Electiva 1 – Bases de datos. App funcional (FastAPI + PostgreSQL 16, 100 % gratuito/open source) que
**diseña, implementa, demuestra y valida** particionamiento horizontal, vertical (y por tanto mixto), replicación síncrona/asíncrona y las decisiones del teorema CAP sobre nodos PostgreSQL independientes.

## Diseño

Dominio: tienda en línea (clientes, pedidos, catálogo).

```
                        ┌────────────────────────── API (FastAPI) ─────────────────────────┐
                        │  router: shard = MD5(cliente_id) mod 3     (shard key)           │
                        └───────┬───────────────┬───────────────┬──────────────┬───────────┘
        HORIZONTAL (filas)      │               │               │      VERTICAL (columnas)
                         ┌──────▼─────┐  ┌──────▼─────┐  ┌──────▼─────┐  ┌─────▼───────────┐
                         │  shard0    │  │  shard1    │  │  shard2    │  │  nodo perfil     │
                         │ clientes_  │  │ (idem)     │  │ (idem)     │  │ clientes_perfil  │
                         │  core      │  │            │  │            │  │ bio, avatar,     │
                         │ pedidos    │  │            │  │            │  │ preferencias     │
                         │ items      │  │            │  │            │  │ (cold / pesado)  │
                         │ productos* │  │ productos* │  │ productos* │  └──────────────────┘
                         └────────────┘  └────────────┘  └────────────┘   * tabla replicada
```

| Técnica | Qué se hizo | Por qué |
|---|---|---|
| **Horizontal** | `clientes_core`, `pedidos`, `pedido_items` se reparten en 3 shards por `hash(cliente_id) mod 3` (MD5, determinista). | Hash ⇒ reparto uniforme sin hot-spots; la shard key es el `cliente_id` porque casi todas las consultas son «de un cliente». |
| **Co-ubicación** | Los pedidos viven en el mismo shard que su cliente. | Transacciones y joins locales; sin 2PC. |
| **Tabla replicada** | `productos` existe completa en cada shard. | Catálogo pequeño y de lectura ⇒ sin joins entre shards. |
| **Vertical** | Las columnas pesadas/frías (`bio`, `avatar`, `preferencias`) salen a `clientes_perfil` en **otro servidor**. | `clientes_core` queda liviana (≈5× menor) y rápida; el perfil se consulta solo bajo demanda. |
| **Scatter-gather** | Consultas sin shard key (por ciudad/email, estadísticas) se envían a todos los shards y se combinan en la app. | Es el costo conocido del sharding; se hace explícito en la respuesta (`_nodos_consultados`). |
| **Replicación** | Cada nodo (3 shards + perfil) tiene una **réplica física** por streaming replication (solo lectura). | Disponibilidad de lectura y tolerancia a fallos de un nodo. |
| **Síncrona / asíncrona** | Conmutable en caliente: `synchronous_commit = remote_apply` + `synchronous_standby_names='*'` (síncrona) o desactivado (asíncrona). Las réplicas aplican con 1 s de retraso (`recovery_min_apply_delay`) para que el lag sea visible. | Muestra el trade-off: síncrona = consistencia fuerte y escrituras más lentas; asíncrona = rápida, consistencia eventual. |
| **Consistencia vertical** | Escritura core→perfil con compensación (saga simple) si falla el segundo paso. | No hay transacción distribuida entre servidores. |

### Teorema CAP: qué decisión toma este sistema

| Situación | Comportamiento | CAP |
|---|---|---|
| Cae un primario y se **lee** | Failover automático a su réplica; la respuesta lleva `_posiblemente_desactualizado: true` | **AP** (responde, quizá con dato viejo) |
| Cae un primario y se **escribe** | Se rechaza con HTTP 503 | **CP** (no arriesga inconsistencia) |
| Modo síncrono y cae la réplica | La escritura se rechaza (en PostgreSQL real el commit queda bloqueado esperando la réplica) | **CP** |
| Modo asíncrono y cae la réplica | Las escrituras siguen funcionando | **AP** |
| Falla el nodo vertical a mitad de un alta | Se compensa (borra el core) | consistencia |

Las caídas de la UI y de las pruebas se **simulan a nivel de aplicación** (flag por nodo) para no depender del socket de Docker;
`test_fallo_real_de_conexion_hace_failover` comprueba además un primario realmente inalcanzable.
Relación con los motores vistos en clase: aquí el reparto lo hace la aplicación sobre PostgreSQL;
CockroachDB (SQL distribuido) y Vitess (MySQL) hacen el sharding/replicación de forma nativa y automática.

Limitaciones (para discutir en la exposición): cambiar el número de shards obliga a re-balancear (con `mod N`
se mueven casi todas las filas; en producción se usaría *consistent hashing* o shards virtuales); la unicidad de
`email` es solo por shard; no hay 2PC.

## Ejecución

**Opción A – Docker** (recomendada): `docker compose up --build` → http://localhost:8000 (8 instancias PostgreSQL: 4 primarios + 4 réplicas; la primera vez las réplicas tardan unos segundos en sincronizar)

**Opción B – sin Docker** (8 instancias PostgreSQL locales: primarios 5441-5444, réplicas 5451-5454):
```bash
scripts/local_cluster.sh start
python -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/uvicorn app.main:app          # UI en http://localhost:8000  ·  docs en /docs
```
(Con Docker, los puertos 5441-5444 y 5451-5454 también quedan expuestos, así que tests y demo corren igual.)

## Demostración

* **UI** (`/`), cuatro pestañas: ① topología con primario/réplica de cada nodo, conteo de filas y distribución;
  ② operaciones (crear, consultar por id, con perfil, por ciudad) mostrando la **ruta de nodos** consultados, leyendo del primario o de la réplica;
  ③ replicación: conmutador síncrona/asíncrona y experimento que mide latencia de escritura y retraso de la réplica;
  ④ fallos: botón «Simular caída» por nodo y prueba de lectura/escritura con la decisión CAP resultante.
* **Consola**: `python -m scripts.demo` → distribución, tamaño de tablas core vs perfil y latencias.
  Salida de ejemplo con 6000 clientes: `2038 / 1985 / 1977` por shard; `clientes_core` 544 kB vs `clientes_perfil` 2424 kB;
  lookup por id 0.5 ms (1 nodo) vs 2.5 ms por ciudad (3 nodos).
* También puede conectarse con `psql -h 127.0.0.1 -p 5441 -U app tienda` a cada shard y ver que cada uno tiene solo su porción.

## Validación

`python -m pytest -q` (16 pruebas contra el clúster real). `tests/test_sharding.py` comprueba:

1. El router es determinista y uniforme (±4 % con 30 000 ids).
2. **Horizontal:** consultando cada nodo *directamente*, toda fila está en el shard que le corresponde y la suma da el total.
3. Los pedidos no quedan huérfanos (co-ubicación).
4. **Vertical:** `clientes_core` no tiene `bio/avatar/preferencias`, el nodo perfil no tiene `clientes_core`, y hay un perfil por cliente.
5. La API consulta solo los nodos necesarios (`shardN` o `shardN`+`perfil`) y el borrado limpia ambos fragmentos.
6. Los agregados scatter-gather (clientes, ventas, búsqueda por ciudad) coinciden con la suma manual por nodo.
7. Un pedido inválido hace rollback local sin dejar basura.
8. Si cae el nodo vertical, se compensa y no queda un cliente a medias.

`tests/test_replicacion.py` comprueba:

9. Las réplicas están en recuperación y rechazan escrituras (solo lectura).
10. **Asíncrona:** tras escribir, la réplica aún no tiene el dato y luego converge (consistencia eventual).
11. **Síncrona:** al responder, la réplica ya lo tiene (consistencia fuerte), con mayor latencia de escritura.
12. El experimento comparativo confirma ambos comportamientos.
13. Failover de lectura a la réplica con primario caído (AP) y rechazo de escrituras (CP), incluido primario realmente inalcanzable.
14. En modo síncrono sin réplica la escritura se bloquea (CP); en asíncrono sigue (AP).

## Estructura
`app/` (router, acceso a datos, API, UI) · `sql/` (esquemas por nodo) · `scripts/` (clúster local, demo) · `tests/`.
