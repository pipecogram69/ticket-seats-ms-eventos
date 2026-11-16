CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE evento (
    id_evento      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    id_ciudad      INT  NOT NULL,
    id_venue       INT  NOT NULL,
    id_organizador UUID NOT NULL,
    nombre         VARCHAR(150) NOT NULL,
    descripcion    TEXT,
    fecha_inicio   TIMESTAMPTZ NOT NULL,
    estado         VARCHAR(10) NOT NULL DEFAULT 'BORRADOR'
                   CHECK (estado IN ('BORRADOR','PUBLICADO','CANCELADO','FINALIZADO')),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_evento_ciudad_fecha ON evento(id_ciudad, fecha_inicio);
CREATE INDEX idx_evento_organizador  ON evento(id_organizador);

CREATE TABLE localidad (
    id_localidad SERIAL PRIMARY KEY,
    id_evento    UUID NOT NULL REFERENCES evento(id_evento) ON DELETE CASCADE,
    nombre       VARCHAR(60) NOT NULL,
    precio       NUMERIC(12,2) NOT NULL CHECK (precio >= 0),
    capacidad    INT NOT NULL CHECK (capacidad > 0),
    UNIQUE (id_evento, nombre)
);

CREATE TABLE boleta (
    id_boleta       BIGSERIAL PRIMARY KEY,
    id_evento       UUID NOT NULL REFERENCES evento(id_evento) ON DELETE CASCADE,
    id_localidad    INT  NOT NULL REFERENCES localidad(id_localidad) ON DELETE CASCADE,
    fila            VARCHAR(5) NOT NULL,
    asiento         INT NOT NULL,
    estado          VARCHAR(10) NOT NULL DEFAULT 'DISPONIBLE'
                    CHECK (estado IN ('DISPONIBLE','RESERVADA','VENDIDA')),
    version         INT NOT NULL DEFAULT 1,
    reservada_por   UUID,
    reservada_hasta TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (id_evento, id_localidad, fila, asiento),
    CHECK (estado <> 'RESERVADA' OR reservada_hasta IS NOT NULL)
);

CREATE INDEX idx_boleta_evento_estado ON boleta(id_evento, id_localidad, estado);
CREATE INDEX idx_boleta_expiracion    ON boleta(reservada_hasta) WHERE estado = 'RESERVADA';

CREATE TABLE condicion_comercial (
  id_evento              UUID PRIMARY KEY REFERENCES evento(id_evento) ON DELETE CASCADE,
  max_boletas_por_compra SMALLINT NOT NULL DEFAULT 6 CHECK (max_boletas_por_compra BETWEEN 1 AND 20),
  minutos_reserva        SMALLINT NOT NULL DEFAULT 10 CHECK (minutos_reserva BETWEEN 1 AND 60),
  inicio_venta           TIMESTAMPTZ,
  fin_venta              TIMESTAMPTZ,
  cargo_servicio_pct     NUMERIC(5,2) NOT NULL DEFAULT 0 CHECK (cargo_servicio_pct BETWEEN 0 AND 100),
  permite_devolucion     BOOLEAN NOT NULL DEFAULT FALSE,
  CHECK (fin_venta IS NULL OR inicio_venta IS NULL OR fin_venta > inicio_venta)
);

ALTER TABLE evento ADD COLUMN IF NOT EXISTS imagen_url VARCHAR(255);

ALTER TABLE evento ADD COLUMN IF NOT EXISTS imagen_url VARCHAR(255);
