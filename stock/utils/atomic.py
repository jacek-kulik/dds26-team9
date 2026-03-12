import redis.asyncio as aioredis
from msgspec import msgpack

async def atomic_update(db: aioredis.Redis, key: str, struct_type, modifier, max_retries: int = 8):
    for attempt in range(max_retries):
        try:
            async with db.pipeline(transaction=False) as pipe:
                await pipe.watch(key)
                raw = await pipe.get(key)

                if raw is None:
                    await pipe.unwatch()
                    return False, None

                entry = msgpack.decode(raw, type=struct_type)
                modified, result = modifier(entry)

                if modified is None:
                    await pipe.unwatch()
                    return False, result

                pipe.multi()
                pipe.set(key, msgpack.encode(modified))
                await pipe.execute()
                return True, result

        except aioredis.WatchError:
            continue

    return False, None