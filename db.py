import asyncpg
from config import CATALOGO_DSN, SHARD_DSNS

catalogo: asyncpg.Pool | None = None
shards: dict[int, asyncpg.Pool] = {}


async def init_pools():
    global catalogo
    catalogo = await asyncpg.create_pool(CATALOGO_DSN, min_size=1, max_size=5)
    for id_shard, dsn in SHARD_DSNS.items():
        shards[id_shard] = await asyncpg.create_pool(dsn, min_size=1, max_size=10)


async def close_pools():
    if catalogo:
        await catalogo.close()
    for pool in shards.values():
        await pool.close()


def shard_pool(id_shard: int) -> asyncpg.Pool:
    return shards[id_shard]
