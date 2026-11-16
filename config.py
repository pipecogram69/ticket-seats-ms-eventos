import os

JWT_SECRET = os.getenv("JWT_SECRET", "cambia-esto-en-produccion")
JWT_ALG = "HS256"

CATALOGO_DSN = os.getenv("CATALOGO_DSN", "postgresql://postgres:postgres@localhost:5434/ticketseats_catalogo")
SHARD_DSNS = {
    1: os.getenv("SHARD1_DSN", "postgresql://postgres:postgres@localhost:5435/ticketseats_shard1"),
    2: os.getenv("SHARD2_DSN", "postgresql://postgres:postgres@localhost:5436/ticketseats_shard2"),
}
MAX_BOLETAS_EVENTO = 5000

UPLOAD_DIR = os.getenv("UPLOAD_DIR", "uploads")
MAX_IMG_BYTES = 3 * 1024 * 1024

HORAS_DURACION_EVENTO = int(os.getenv("HORAS_DURACION_EVENTO", "6"))
