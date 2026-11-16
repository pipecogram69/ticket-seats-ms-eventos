from datetime import datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field


class LocalidadIn(BaseModel):
    nombre: str = Field(min_length=1, max_length=60)
    precio: Decimal = Field(ge=0, le=100000000)
    filas: int = Field(ge=1, le=26)
    asientos_por_fila: int = Field(ge=1, le=100)


class CondicionesIn(BaseModel):
    max_boletas_por_compra: int = Field(default=6, ge=1, le=20)
    minutos_reserva: int = Field(default=10, ge=1, le=60)
    inicio_venta: datetime | None = None
    fin_venta: datetime | None = None
    cargo_servicio_pct: Decimal = Field(default=Decimal(0), ge=0, le=100)
    permite_devolucion: bool = False


class EventoIn(BaseModel):
    id_venue: int
    nombre: str = Field(min_length=3, max_length=150)
    descripcion: str | None = None
    fecha_inicio: datetime
    publicar: bool = True
    localidades: list[LocalidadIn] = Field(min_length=1, max_length=10)
    condiciones: CondicionesIn = Field(default_factory=CondicionesIn)


class CambioEstadoEvento(BaseModel):
    estado: Literal["PUBLICADO", "CANCELADO"]


class BoletaVersion(BaseModel):
    id_boleta: int
    version: int


class ReservaIn(BaseModel):
    id_evento: UUID
    boletas: list[BoletaVersion] = Field(min_length=1, max_length=20)


class IdsIn(BaseModel):
    id_evento: UUID
    ids: list[int] = Field(min_length=1, max_length=20)


class EventoEdit(BaseModel):
    nombre: str | None = Field(default=None, min_length=3, max_length=150)
    descripcion: str | None = None
    fecha_inicio: datetime | None = None
    max_boletas_por_compra: int | None = Field(default=None, ge=1, le=20)
    minutos_reserva: int | None = Field(default=None, ge=1, le=60)
    inicio_venta: datetime | None = None
    fin_venta: datetime | None = None
    cargo_servicio_pct: Decimal | None = Field(default=None, ge=0, le=100)
    permite_devolucion: bool | None = None
