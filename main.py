import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

import db
from auth import requiere_roles, usuario_actual
from config import MAX_BOLETAS_EVENTO
from schemas import CambioEstadoEvento, EventoIn, IdsIn, ReservaIn


def aware(dt):
    if dt is None:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


async def liberar_expiradas():
    """Cada 60 s devuelve a DISPONIBLE las reservas vencidas de todos los shards."""
    while True:
        await asyncio.sleep(60)
        for sid, pool in db.shards.items():
            try:
                async with pool.acquire() as conn:
                    await conn.execute(
                        """UPDATE boleta SET estado = 'DISPONIBLE', version = version + 1,
                                  reservada_por = NULL, reservada_hasta = NULL, updated_at = now()
                           WHERE estado = 'RESERVADA' AND reservada_hasta < now()""")
            except Exception as e:
                print(f"[expiracion] shard {sid}: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.init_pools()
    tarea = asyncio.create_task(liberar_expiradas())
    tarea2 = asyncio.create_task(finalizar_eventos())
    yield
    tarea.cancel()
    tarea2.cancel()
    await db.close_pools()


app = FastAPI(title="MS Eventos e Inventario", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


# ---------- Enrutamiento por shard ----------
async def shard_de_ciudad(id_ciudad: int) -> int:
    async with db.catalogo.acquire() as c:
        sid = await c.fetchval("SELECT id_shard FROM ciudad WHERE id_ciudad = $1", id_ciudad)
    if sid is None:
        raise HTTPException(404, "La ciudad no existe")
    return sid


async def shard_de_evento(id_evento: UUID) -> int:
    async with db.catalogo.acquire() as c:
        sid = await c.fetchval("SELECT id_shard FROM evento_ruta WHERE id_evento = $1", id_evento)
    if sid is None:
        raise HTTPException(404, "Evento no encontrado")
    return sid


async def nombres_catalogo():
    async with db.catalogo.acquire() as c:
        ciudades = {r["id_ciudad"]: r["nombre"] for r in await c.fetch("SELECT id_ciudad, nombre FROM ciudad")}
        venues = {r["id_venue"]: r["nombre"] for r in await c.fetch("SELECT id_venue, nombre FROM venue")}
    return ciudades, venues


async def scatter(sql: str, params: tuple, ids_shard: list[int]) -> list[dict]:
    """Ejecuta la consulta en cada shard indicado y junta los resultados."""
    async def uno(sid):
        async with db.shard_pool(sid).acquire() as conn:
            return [{**dict(r), "id_shard": sid} for r in await conn.fetch(sql, *params)]
    partes = await asyncio.gather(*(uno(s) for s in ids_shard))
    return sorted((x for p in partes for x in p), key=lambda x: x["fecha_inicio"])


def sql_eventos(propios: bool) -> str:
    filtro = "e.id_organizador = $3::uuid" if propios else "e.estado = 'PUBLICADO'"
    return f"""
        SELECT e.id_evento::text AS id_evento, e.nombre, e.fecha_inicio, e.estado, e.id_ciudad, e.id_venue, e.imagen_url,
               (SELECT min(l.precio) FROM localidad l WHERE l.id_evento = e.id_evento) AS precio_desde,
               (SELECT count(*) FROM boleta b WHERE b.id_evento = e.id_evento
                  AND (b.estado = 'DISPONIBLE' OR (b.estado = 'RESERVADA' AND b.reservada_hasta < now()))) AS disponibles
        FROM evento e
        WHERE {filtro}
          AND ($1::int IS NULL OR e.id_ciudad = $1)
          AND ($2::text IS NULL OR e.nombre ILIKE '%' || $2 || '%')
        ORDER BY e.fecha_inicio LIMIT 100"""


async def con_nombres(filas: list[dict]) -> list[dict]:
    ciudades, venues = await nombres_catalogo()
    for f in filas:
        f["ciudad"] = ciudades.get(f["id_ciudad"])
        f["venue"] = venues.get(f["id_venue"])
    return filas


# ---------- Catálogo ----------
@app.get("/health")
async def health():
    return {"ok": True}


@app.get("/ciudades")
async def ciudades():
    async with db.catalogo.acquire() as c:
        rows = await c.fetch("SELECT id_ciudad, nombre, id_shard FROM ciudad ORDER BY nombre")
    return [dict(r) for r in rows]


@app.get("/venues")
async def venues(id_ciudad: int | None = None):
    async with db.catalogo.acquire() as c:
        rows = await c.fetch(
            """SELECT v.id_venue, v.nombre, v.aforo, c.id_ciudad, c.nombre AS ciudad, c.id_shard
               FROM venue v JOIN ciudad c ON c.id_ciudad = v.id_ciudad
               WHERE ($1::int IS NULL OR c.id_ciudad = $1)
               ORDER BY c.nombre, v.nombre""", id_ciudad)
    return [dict(r) for r in rows]


@app.get("/shards")
async def estado_shards(_=Depends(requiere_roles("ADMIN"))):
    async def uno(sid, pool):
        async with pool.acquire() as c:
            eventos = await c.fetchval("SELECT count(*) FROM evento")
            filas = await c.fetch("SELECT estado, count(*) AS n FROM boleta GROUP BY estado")
        return {"id_shard": sid, "eventos": eventos, "boletas": {r["estado"]: r["n"] for r in filas}}

    res = await asyncio.gather(*(uno(s, p) for s, p in db.shards.items()))
    async with db.catalogo.acquire() as c:
        ciud = await c.fetch("SELECT nombre, id_shard FROM ciudad ORDER BY nombre")
    for r in res:
        r["ciudades"] = [x["nombre"] for x in ciud if x["id_shard"] == r["id_shard"]]
    return res


# ---------- Eventos ----------
@app.post("/eventos", status_code=201)
async def crear_evento(data: EventoIn, user=Depends(requiere_roles("ORGANIZADOR", "ADMIN"))):
    fecha = aware(data.fecha_inicio)
    if fecha <= datetime.now(timezone.utc):
        raise HTTPException(422, "La fecha del evento debe ser futura")
    cond = data.condiciones
    ini, fin = aware(cond.inicio_venta), aware(cond.fin_venta)
    if ini and fin and fin <= ini:
        raise HTTPException(422, "El fin de la venta debe ser posterior al inicio")
    nombres = [l.nombre.strip().lower() for l in data.localidades]
    if len(set(nombres)) != len(nombres):
        raise HTTPException(422, "Los nombres de las localidades no pueden repetirse")

    async with db.catalogo.acquire() as c:
        v = await c.fetchrow(
            """SELECT v.id_venue, v.id_ciudad, v.aforo, ci.id_shard
               FROM venue v JOIN ciudad ci ON ci.id_ciudad = v.id_ciudad
               WHERE v.id_venue = $1""", data.id_venue)
    if not v:
        raise HTTPException(404, "El escenario no existe")

    total = sum(l.filas * l.asientos_por_fila for l in data.localidades)
    if total > v["aforo"]:
        raise HTTPException(422, f"Las localidades suman {total} asientos y el aforo del escenario es {v['aforo']}")
    if total > MAX_BOLETAS_EVENTO:
        raise HTTPException(422, f"Máximo {MAX_BOLETAS_EVENTO} asientos por evento")

    pool = db.shard_pool(v["id_shard"])
    async with pool.acquire() as conn:
        async with conn.transaction():
            id_evento = await conn.fetchval(
                """INSERT INTO evento (id_ciudad, id_venue, id_organizador, nombre, descripcion, fecha_inicio, estado)
                   VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id_evento""",
                v["id_ciudad"], v["id_venue"], UUID(user["sub"]), data.nombre, data.descripcion,
                fecha, "PUBLICADO" if data.publicar else "BORRADOR")
            await conn.execute(
                """INSERT INTO condicion_comercial (id_evento, max_boletas_por_compra, minutos_reserva,
                          inicio_venta, fin_venta, cargo_servicio_pct, permite_devolucion)
                   VALUES ($1, $2, $3, $4, $5, $6, $7)""",
                id_evento, cond.max_boletas_por_compra, cond.minutos_reserva, ini, fin,
                cond.cargo_servicio_pct, cond.permite_devolucion)
            for loc in data.localidades:
                id_loc = await conn.fetchval(
                    """INSERT INTO localidad (id_evento, nombre, precio, capacidad)
                       VALUES ($1, $2, $3, $4) RETURNING id_localidad""",
                    id_evento, loc.nombre.strip(), loc.precio, loc.filas * loc.asientos_por_fila)
                await conn.execute(
                    """INSERT INTO boleta (id_evento, id_localidad, fila, asiento)
                       SELECT $1::uuid, $2::int, chr(64 + f), a
                       FROM generate_series(1, $3::int) f, generate_series(1, $4::int) a""",
                    id_evento, id_loc, loc.filas, loc.asientos_por_fila)

    try:
        async with db.catalogo.acquire() as c:
            await c.execute(
                "INSERT INTO evento_ruta (id_evento, id_shard, id_ciudad, id_venue) VALUES ($1, $2, $3, $4)",
                id_evento, v["id_shard"], v["id_ciudad"], v["id_venue"])
    except Exception:
        async with pool.acquire() as conn:      # compensación: deshacer en el shard
            await conn.execute("DELETE FROM evento WHERE id_evento = $1", id_evento)
        raise HTTPException(500, "No se pudo registrar la ruta del evento; se revirtió la creación")

    return {"id_evento": str(id_evento), "id_shard": v["id_shard"], "boletas_generadas": total}


@app.get("/eventos")
async def listar_eventos(id_ciudad: int | None = None, q: str | None = None):
    ids = [await shard_de_ciudad(id_ciudad)] if id_ciudad else list(db.shards.keys())
    return await con_nombres(await scatter(sql_eventos(False), (id_ciudad, q), ids))


@app.get("/eventos/mios")
async def mis_eventos(user=Depends(requiere_roles("ORGANIZADOR", "ADMIN"))):
    filas = await scatter(sql_eventos(True), (None, None, UUID(user["sub"])), list(db.shards.keys()))
    return await con_nombres(filas)


@app.get("/eventos/{id_evento}")
async def detalle_evento(id_evento: UUID):
    sid = await shard_de_evento(id_evento)
    async with db.shard_pool(sid).acquire() as conn:
        e = await conn.fetchrow(
            """SELECT id_evento::text AS id_evento, id_ciudad, id_venue, id_organizador::text AS id_organizador,
                      nombre, descripcion, fecha_inicio, estado, imagen_url
               FROM evento WHERE id_evento = $1""", id_evento)
        if not e:
            raise HTTPException(404, "Evento no encontrado")
        cond = await conn.fetchrow("SELECT * FROM condicion_comercial WHERE id_evento = $1", id_evento)
        locs = await conn.fetch(
            """SELECT l.id_localidad, l.nombre, l.precio, l.capacidad,
                      count(*) FILTER (WHERE b.estado = 'DISPONIBLE'
                         OR (b.estado = 'RESERVADA' AND b.reservada_hasta < now())) AS disponibles
               FROM localidad l LEFT JOIN boleta b ON b.id_localidad = l.id_localidad
               WHERE l.id_evento = $1 GROUP BY l.id_localidad ORDER BY l.id_localidad""", id_evento)
    ciudades, venues = await nombres_catalogo()
    return {**dict(e), "ciudad": ciudades.get(e["id_ciudad"]), "venue": venues.get(e["id_venue"]),
            "id_shard": sid, "condiciones": dict(cond) if cond else None,
            "localidades": [dict(l) for l in locs]}


@app.patch("/eventos/{id_evento}/estado")
async def cambiar_estado_evento(id_evento: UUID, data: CambioEstadoEvento,
                                user=Depends(requiere_roles("ORGANIZADOR", "ADMIN"))):
    sid = await shard_de_evento(id_evento)
    async with db.shard_pool(sid).acquire() as conn:
        e = await conn.fetchrow(
            "SELECT id_organizador::text AS org, estado FROM evento WHERE id_evento = $1", id_evento)
        if not e:
            raise HTTPException(404, "Evento no encontrado")
        if user["rol"] != "ADMIN" and e["org"] != user["sub"]:
            raise HTTPException(403, "Solo el organizador del evento puede cambiar su estado")
        if e["estado"] in ("CANCELADO", "FINALIZADO"):
            raise HTTPException(409, f"El evento ya está {e['estado']}")
        if data.estado == "PUBLICADO" and e["estado"] != "BORRADOR":
            raise HTTPException(409, "Solo un borrador se puede publicar")
        await conn.execute("UPDATE evento SET estado = $1, updated_at = now() WHERE id_evento = $2",
                           data.estado, id_evento)
    return {"id_evento": str(id_evento), "estado": data.estado}


@app.get("/eventos/{id_evento}/boletas")
async def listar_boletas(id_evento: UUID, id_localidad: int | None = None):
    sid = await shard_de_evento(id_evento)
    async with db.shard_pool(sid).acquire() as conn:
        rows = await conn.fetch(
            """SELECT b.id_boleta, b.id_localidad, l.nombre AS localidad, l.precio, b.fila, b.asiento,
                      CASE WHEN b.estado = 'RESERVADA' AND b.reservada_hasta < now()
                           THEN 'DISPONIBLE' ELSE b.estado END AS estado,
                      b.version
               FROM boleta b JOIN localidad l ON l.id_localidad = b.id_localidad
               WHERE b.id_evento = $1 AND ($2::int IS NULL OR b.id_localidad = $2)
               ORDER BY b.id_localidad, b.fila, b.asiento""", id_evento, id_localidad)
    return [dict(r) for r in rows]


# ---------- Inventario (bloqueo optimista) ----------
@app.post("/inventario/reservar")
async def reservar(data: ReservaIn, user=Depends(requiere_roles("CLIENTE", "ADMIN"))):
    ids = [b.id_boleta for b in data.boletas]
    if len(set(ids)) != len(ids):
        raise HTTPException(422, "Hay boletas repetidas en la solicitud")
    sid = await shard_de_evento(data.id_evento)
    uid = UUID(user["sub"])
    ahora = datetime.now(timezone.utc)

    async with db.shard_pool(sid).acquire() as conn:
        ev = await conn.fetchrow(
            """SELECT e.estado, e.fecha_inicio, c.max_boletas_por_compra, c.minutos_reserva,
                      c.inicio_venta, c.fin_venta, c.cargo_servicio_pct
               FROM evento e LEFT JOIN condicion_comercial c ON c.id_evento = e.id_evento
               WHERE e.id_evento = $1""", data.id_evento)
        if not ev:
            raise HTTPException(404, "Evento no encontrado")
        if ev["estado"] != "PUBLICADO":
            raise HTTPException(409, "El evento no está disponible para la venta")
        if ev["fecha_inicio"] <= ahora:
            raise HTTPException(409, "El evento ya inició")
        if ev["inicio_venta"] and ahora < ev["inicio_venta"]:
            raise HTTPException(409, "La venta aún no ha comenzado")
        if ev["fin_venta"] and ahora > ev["fin_venta"]:
            raise HTTPException(409, "La venta ya terminó")

        maxb = ev["max_boletas_por_compra"] or 6
        mins = ev["minutos_reserva"] or 10
        cargo_pct = ev["cargo_servicio_pct"] or Decimal(0)

        activas = await conn.fetchval(
            """SELECT count(*) FROM boleta WHERE id_evento = $1 AND reservada_por = $2
               AND estado = 'RESERVADA' AND reservada_hasta > now()""", data.id_evento, uid)
        if activas + len(ids) > maxb:
            raise HTTPException(409, f"Máximo {maxb} boletas por compra (ya tienes {activas} reservadas)")

        async with conn.transaction():
            for b in sorted(data.boletas, key=lambda x: x.id_boleta):   # orden fijo evita deadlocks
                ok = await conn.fetchval(
                    """UPDATE boleta
                       SET estado = 'RESERVADA', version = version + 1, reservada_por = $1,
                           reservada_hasta = now() + make_interval(mins => $2::int), updated_at = now()
                       WHERE id_boleta = $3 AND id_evento = $4 AND version = $5
                         AND (estado = 'DISPONIBLE' OR (estado = 'RESERVADA' AND reservada_hasta < now()))
                       RETURNING 1""",
                    uid, mins, b.id_boleta, data.id_evento, b.version)
                if not ok:
                    raise HTTPException(
                        409, f"La boleta {b.id_boleta} ya no está disponible o fue modificada por otro usuario")
            filas = await conn.fetch(
                """SELECT b.id_boleta, l.nombre AS localidad, b.fila, b.asiento, l.precio, b.version, b.reservada_hasta
                   FROM boleta b JOIN localidad l ON l.id_localidad = b.id_localidad
                   WHERE b.id_boleta = ANY($1::bigint[]) ORDER BY b.id_boleta""", ids)

    subtotal = sum((f["precio"] for f in filas), Decimal(0))
    cargo = (subtotal * cargo_pct / 100).quantize(Decimal("0.01"))
    return {"id_evento": str(data.id_evento), "reservadas": [dict(f) for f in filas],
            "subtotal": subtotal, "cargo_servicio": cargo, "total": subtotal + cargo,
            "expira_en": min(f["reservada_hasta"] for f in filas)}


@app.post("/inventario/liberar")
async def liberar(data: IdsIn, user=Depends(usuario_actual)):
    sid = await shard_de_evento(data.id_evento)
    async with db.shard_pool(sid).acquire() as conn:
        n = await conn.fetchval(
            """WITH x AS (
                 UPDATE boleta SET estado = 'DISPONIBLE', version = version + 1,
                        reservada_por = NULL, reservada_hasta = NULL, updated_at = now()
                 WHERE id_boleta = ANY($1::bigint[]) AND id_evento = $2 AND estado = 'RESERVADA'
                   AND ($3::boolean OR reservada_por = $4::uuid)
                 RETURNING 1)
               SELECT count(*) FROM x""",
            data.ids, data.id_evento, user["rol"] == "ADMIN", UUID(user["sub"]))
    return {"liberadas": n}


@app.post("/inventario/confirmar")
async def confirmar(data: IdsIn, user=Depends(requiere_roles("CLIENTE", "ADMIN"))):
    sid = await shard_de_evento(data.id_evento)
    async with db.shard_pool(sid).acquire() as conn:
        async with conn.transaction():
            rows = await conn.fetch(
                """UPDATE boleta SET estado = 'VENDIDA', version = version + 1,
                          reservada_hasta = NULL, updated_at = now()
                   WHERE id_boleta = ANY($1::bigint[]) AND id_evento = $2 AND estado = 'RESERVADA'
                     AND reservada_por = $3 AND reservada_hasta > now()
                   RETURNING id_boleta""", data.ids, data.id_evento, UUID(user["sub"]))
            if len(rows) != len(set(data.ids)):
                raise HTTPException(409, "Alguna reserva expiró o no te pertenece; no se confirmó nada")
    return {"vendidas": [r["id_boleta"] for r in rows]}


# ---------- Imágenes de eventos ----------
import os
from io import BytesIO
from uuid import uuid4

import cloudinary
from cloudinary import uploader

from fastapi import File, UploadFile
from fastapi.staticfiles import StaticFiles

from config import MAX_IMG_BYTES, UPLOAD_DIR

# Se conserva el directorio para poder servir imágenes antiguas
os.makedirs(UPLOAD_DIR, exist_ok=True)
app.mount("/imagenes", StaticFiles(directory=UPLOAD_DIR), name="imagenes")


def tipo_imagen(b: bytes) -> str | None:
    if b[:3] == b"\xff\xd8\xff":
        return ".jpg"
    if b[:8] == b"\x89PNG\r\n\x1a\n":
        return ".png"
    if b[:4] == b"RIFF" and b[8:12] == b"WEBP":
        return ".webp"
    return None


@app.post("/eventos/{id_evento}/imagen")
async def subir_imagen(id_evento: UUID, archivo: UploadFile = File(...),
                       user=Depends(requiere_roles("ORGANIZADOR", "ADMIN"))):
    sid = await shard_de_evento(id_evento)

    data = await archivo.read(MAX_IMG_BYTES + 1)

    if len(data) > MAX_IMG_BYTES:
        raise HTTPException(413, "La imagen supera 3 MB")

    ext = tipo_imagen(data)
    if not ext:
        raise HTTPException(415, "Formato no permitido: usa JPG, PNG o WEBP")

    cloud_name = os.getenv("CLOUDINARY_CLOUD_NAME")
    api_key = os.getenv("CLOUDINARY_API_KEY")
    api_secret = os.getenv("CLOUDINARY_API_SECRET")

    if not all([cloud_name, api_key, api_secret]):
        raise HTTPException(503, "El almacenamiento de imágenes no está configurado")

    async with db.shard_pool(sid).acquire() as conn:
        e = await conn.fetchrow(
            "SELECT id_organizador::text AS org, imagen_url FROM evento WHERE id_evento = $1",
            id_evento
        )

        if not e:
            raise HTTPException(404, "Evento no encontrado")

        if user["rol"] != "ADMIN" and e["org"] != user["sub"]:
            raise HTTPException(403, "Solo el organizador del evento puede cambiar su imagen")

    try:
        cloudinary.config(
            cloud_name=cloud_name,
            api_key=api_key,
            api_secret=api_secret,
            secure=True
        )

        resultado = await asyncio.to_thread(
            uploader.upload,
            BytesIO(data),
            folder="ticketseats/eventos",
            public_id=uuid4().hex,
            resource_type="image"
        )

        url = resultado.get("secure_url")

        if not url:
            raise RuntimeError("Cloudinary no devolvió una URL segura")

    except Exception as exc:
        print(f"[cloudinary] Error al subir imagen: {exc}")
        raise HTTPException(502, "No se pudo guardar la imagen en Cloudinary")

    async with db.shard_pool(sid).acquire() as conn:
        await conn.execute(
            "UPDATE evento SET imagen_url = $1, updated_at = now() WHERE id_evento = $2",
            url,
            id_evento
        )

    return {"imagen_url": url}


# ---------- Reservas activas del usuario (las usa el MS de órdenes) ----------
@app.get("/inventario/mis-reservas/{id_evento}")
async def mis_reservas(id_evento: UUID, user=Depends(requiere_roles("CLIENTE", "ADMIN"))):
    sid = await shard_de_evento(id_evento)
    async with db.shard_pool(sid).acquire() as conn:
        pct = await conn.fetchval(
            "SELECT cargo_servicio_pct FROM condicion_comercial WHERE id_evento = $1", id_evento)
        filas = await conn.fetch(
            """SELECT b.id_boleta, l.nombre AS localidad, b.fila, b.asiento, l.precio, b.reservada_hasta
               FROM boleta b JOIN localidad l ON l.id_localidad = b.id_localidad
               WHERE b.id_evento = $1 AND b.reservada_por = $2
                 AND b.estado = 'RESERVADA' AND b.reservada_hasta > now()
               ORDER BY b.id_boleta""", id_evento, UUID(user["sub"]))
    return {"cargo_servicio_pct": pct or Decimal(0), "boletas": [dict(f) for f in filas]}


# ---------- Edición, finalización automática, devolución de inventario y resumen del organizador ----------
import asyncpg

from config import HORAS_DURACION_EVENTO
from schemas import EventoEdit

CAMPOS_EVENTO = ("nombre", "descripcion", "fecha_inicio")
CAMPOS_COND = ("max_boletas_por_compra", "minutos_reserva", "inicio_venta", "fin_venta",
               "cargo_servicio_pct", "permite_devolucion")
ANULABLES = ("descripcion", "inicio_venta", "fin_venta")


async def finalizar_eventos():
    """Pasa a FINALIZADO los eventos publicados cuya duración ya terminó."""
    while True:
        for sid, pool in db.shards.items():
            try:
                async with pool.acquire() as conn:
                    await conn.execute(
                        """UPDATE evento SET estado = 'FINALIZADO', updated_at = now()
                           WHERE estado = 'PUBLICADO'
                             AND fecha_inicio + make_interval(hours => $1::int) < now()""",
                        HORAS_DURACION_EVENTO)
            except Exception as e:
                print(f"[finalizar] shard {sid}: {e}")
        await asyncio.sleep(300)


def sql_set(campos: dict) -> tuple[str, list]:
    cols = ", ".join(f"{k} = ${i}" for i, k in enumerate(campos, start=1))
    return cols, list(campos.values())


@app.patch("/eventos/{id_evento}")
async def editar_evento(id_evento: UUID, data: EventoEdit,
                        user=Depends(requiere_roles("ORGANIZADOR", "ADMIN"))):
    cambios = data.model_dump(exclude_unset=True)
    if not cambios:
        raise HTTPException(422, "No enviaste ningún cambio")
    for k, v in cambios.items():
        if v is None and k not in ANULABLES:
            raise HTTPException(422, f"El campo {k} no puede ser nulo")
    for k in ("fecha_inicio", "inicio_venta", "fin_venta"):
        if cambios.get(k) is not None:
            cambios[k] = aware(cambios[k])
    if cambios.get("fecha_inicio") and cambios["fecha_inicio"] <= datetime.now(timezone.utc):
        raise HTTPException(422, "La fecha del evento debe ser futura")
    ev_c = {k: cambios[k] for k in CAMPOS_EVENTO if k in cambios}
    co_c = {k: cambios[k] for k in CAMPOS_COND if k in cambios}

    sid = await shard_de_evento(id_evento)
    try:
        async with db.shard_pool(sid).acquire() as conn:
            async with conn.transaction():
                e = await conn.fetchrow(
                    "SELECT id_organizador::text AS org, estado FROM evento WHERE id_evento = $1 FOR UPDATE",
                    id_evento)
                if not e:
                    raise HTTPException(404, "Evento no encontrado")
                if user["rol"] != "ADMIN" and e["org"] != user["sub"]:
                    raise HTTPException(403, "Solo el organizador del evento puede editarlo")
                if e["estado"] in ("CANCELADO", "FINALIZADO"):
                    raise HTTPException(409, f"Un evento {e['estado']} no se puede editar")
                if ev_c:
                    cols, vals = sql_set(ev_c)
                    await conn.execute(
                        f"UPDATE evento SET {cols}, updated_at = now() WHERE id_evento = ${len(vals) + 1}",
                        *vals, id_evento)
                if co_c:
                    cols, vals = sql_set(co_c)
                    await conn.execute(
                        f"UPDATE condicion_comercial SET {cols} WHERE id_evento = ${len(vals) + 1}",
                        *vals, id_evento)
    except asyncpg.CheckViolationError:
        raise HTTPException(422, "El fin de la venta debe ser posterior al inicio")
    return {"id_evento": str(id_evento), "actualizado": True}


@app.post("/inventario/devolver")
async def devolver_inventario(data: IdsIn, user=Depends(requiere_roles("CLIENTE", "ADMIN"))):
    """Vuelve a DISPONIBLE asientos VENDIDOS (lo usa el MS de órdenes al devolver una boleta)."""
    sid = await shard_de_evento(data.id_evento)
    async with db.shard_pool(sid).acquire() as conn:
        async with conn.transaction():
            rows = await conn.fetch(
                """UPDATE boleta SET estado = 'DISPONIBLE', version = version + 1,
                          reservada_por = NULL, reservada_hasta = NULL, updated_at = now()
                   WHERE id_boleta = ANY($1::bigint[]) AND id_evento = $2 AND estado = 'VENDIDA'
                     AND ($3::boolean OR reservada_por = $4::uuid)
                   RETURNING id_boleta""",
                data.ids, data.id_evento, user["rol"] == "ADMIN", UUID(user["sub"]))
            if len(rows) != len(set(data.ids)):
                raise HTTPException(409, "Alguna boleta no está vendida a tu nombre")
    return {"devueltas": [r["id_boleta"] for r in rows]}


SQL_RESUMEN = """
    SELECT e.id_evento::text AS id_evento, e.nombre, e.fecha_inicio, e.estado, e.id_ciudad, e.id_venue, e.imagen_url,
           count(b.id_boleta) AS total,
           count(*) FILTER (WHERE b.estado = 'VENDIDA') AS vendidas,
           count(*) FILTER (WHERE b.estado = 'RESERVADA' AND b.reservada_hasta > now()) AS reservadas,
           coalesce(sum(l.precio) FILTER (WHERE b.estado = 'VENDIDA'), 0) AS ingresos
    FROM evento e
    LEFT JOIN boleta b ON b.id_evento = e.id_evento
    LEFT JOIN localidad l ON l.id_localidad = b.id_localidad
    WHERE ($1::uuid IS NULL OR e.id_organizador = $1)
    GROUP BY e.id_evento
    ORDER BY e.fecha_inicio DESC"""


@app.get("/organizador/eventos")
async def eventos_del_organizador(user=Depends(requiere_roles("ORGANIZADOR", "ADMIN"))):
    org = None if user["rol"] == "ADMIN" else UUID(user["sub"])
    filas = await scatter(SQL_RESUMEN, (org,), list(db.shards.keys()))
    filas.sort(key=lambda f: f["fecha_inicio"], reverse=True)
    return await con_nombres(filas)
