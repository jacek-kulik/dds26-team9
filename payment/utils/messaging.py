import os
import time
import logging
import socket
import redis

logger = logging.getLogger(__name__)

bus_db = redis.Redis(host=os.environ['BUS_REDIS_HOST'],
                              port=int(os.environ['BUS_REDIS_PORT']),
                              password=os.environ['BUS_REDIS_PASSWORD'],
                              db=int(os.environ['BUS_REDIS_DB']))

_SERVICE_NAME = os.environ.get("SERVICE_NAME", "unknown")
_PRODUCER_ID = f"{_SERVICE_NAME}-{socket.gethostname()}-{os.getpid()}"

STREAMS = {
    "order":   "order.events",
    "stock":   "stock.events",
    "payment": "payment.events",
}

STREAM_MAX_LEN = 50000
def config_idmp(stream: str):
    try:
        bus_db.execute_command(
            "XCFGSET", stream,
            "IDMP-DURATION", str(120),
            "IDMP-MAXSIZE", str(1000),
        )
    except redis.exceptions.ResponseError as e:
        logger.warning(f"Could not configure idempotency for {stream} -- {e}")
    except redis.exceptions.ConnectionError:
        pass

GROUPS = {
    "order":   "order-workers",
    "stock":   "stock-workers",
    "payment": "payment-workers",
}

def ensure_group(service: str, retries: int = 30, delay: float = 1.0):
    for attempt in range(retries):
        try:
            bus_db.xgroup_create(STREAMS[service], GROUPS[service], id="0", mkstream=True)
            config_idmp(STREAMS[service])
            return
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" in str(e):
                config_idmp(STREAMS[service])
                return
            raise
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
            logger.warning(f"[{service}] Bus Redis not ready.")
            time.sleep(delay)
    logger.error(f"[{service}] Could not connect to bus Redis after {retries} tries.")


def publish(target_service: str, data: dict):
    stream = STREAMS[target_service]
    try:
        args = ["XADD", stream, "IDMPAUTO", _PRODUCER_ID, "*", "MAXLEN", "~", str(STREAM_MAX_LEN)]
        for k, v in data.items():
            args.append(k)
            args.append(v)
        bus_db.execute_command(*args)
    except redis.exceptions.ResponseError:
        bus_db.xadd(stream, data, maxlen=STREAM_MAX_LEN, approximate=True)


def consume(service: str, worker_id: str, batch: int = 10):
    stream = STREAMS[service]
    group  = GROUPS[service]
    while True:
        try:
            results = bus_db.xreadgroup(
                groupname=group,
                consumername=worker_id,
                streams={stream: ">"},
                count=batch,
                block=100,
            )
            if results:
                for _, messages in results:
                    for msg_id, data in messages:
                        yield msg_id, data
        except redis.exceptions.RedisError as e:
            logger.error(f"[{service}] Redis error in consume: {e}")
            time.sleep(0.1)


def ack(service: str, msg_id):
    bus_db.xack(STREAMS[service], GROUPS[service], msg_id)
