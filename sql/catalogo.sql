CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE shard (
    id_shard SMALLINT PRIMARY KEY,
    nombre   VARCHAR(40) NOT NULL UNIQUE,
    activo   BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE ciudad (
    id_ciudad SERIAL PRIMARY KEY,
    nombre    VARCHAR(80) NOT NULL UNIQUE,
    id_shard  SMALLINT NOT NULL REFERENCES shard(id_shard)
);

CREATE TABLE venue (
    id_venue  SERIAL PRIMARY KEY,
    id_ciudad INT NOT NULL REFERENCES ciudad(id_ciudad),
    nombre    VARCHAR(120) NOT NULL,
    direccion VARCHAR(200),
    aforo     INT NOT NULL CHECK (aforo > 0),
    UNIQUE (id_ciudad, nombre)
);

CREATE TABLE evento_ruta (
    id_evento  UUID PRIMARY KEY,
    id_shard   SMALLINT NOT NULL REFERENCES shard(id_shard),
    id_ciudad  INT NOT NULL REFERENCES ciudad(id_ciudad),
    id_venue   INT NOT NULL REFERENCES venue(id_venue),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_evento_ruta_ciudad ON evento_ruta(id_ciudad);

INSERT INTO shard (id_shard, nombre) VALUES (1, 'shard_1'), (2, 'shard_2');

INSERT INTO ciudad (nombre, id_shard) VALUES
 ('Bogotá', 1), ('Barranquilla', 1), ('Medellín', 2), ('Cali', 2);

INSERT INTO venue (id_ciudad, nombre, direccion, aforo)
SELECT c.id_ciudad, v.nombre, v.direccion, v.aforo FROM (VALUES
 ('Bogotá',       'Movistar Arena',  'Bogotá',       14000),
 ('Bogotá',       'Teatro Colsubsidio', 'Bogotá',     1000),
 ('Barranquilla', 'Estadio Metropolitano', 'Barranquilla', 3000),
 ('Medellín',     'Teatro Metropolitano', 'Medellín', 1600),
 ('Medellín',     'Plaza Mayor',     'Medellín',      5000),
 ('Cali',         'Teatro Jorge Isaacs', 'Cali',      1100)
) AS v(ciudad, nombre, direccion, aforo)
JOIN ciudad c ON c.nombre = v.ciudad;

INSERT INTO venue (id_ciudad, nombre, direccion, aforo)
SELECT c.id_ciudad, v.nombre, v.direccion, v.aforo FROM (VALUES
 ('Bogotá',   'Medplus',                   'Bogotá',   8000),
 ('Bogotá',   'Estadio El Campín',         'Bogotá',  36000),
 ('Bogotá',   'Vive Claro',                'Bogotá',  15000),
 ('Medellín', 'Estadio Atanasio Girardot', 'Medellín', 45000)
) AS v(ciudad, nombre, direccion, aforo)
JOIN ciudad c ON c.nombre = v.ciudad
ON CONFLICT (id_ciudad, nombre) DO NOTHING;
