import redis
from msgspec import msgpack

def atomic_update(db: redis.Redis, key: str, struct_type, modifier, max_retries: int = 8):
    for attempt in range(max_retries):
        try:
            with db.pipeline() as pipe:
                pipe.watch(key)
                raw = pipe.get(key)

                if raw is None:
                    pipe.unwatch()
                    return False, None

                entry = msgpack.decode(raw, type=struct_type)
                modified, result = modifier(entry)

                if modified is None:
                    pipe.unwatch()
                    return False, result

                pipe.multi()
                pipe.set(key, msgpack.encode(modified))
                pipe.execute()
                return True, result

        except redis.WatchError:
            continue

    return False, None