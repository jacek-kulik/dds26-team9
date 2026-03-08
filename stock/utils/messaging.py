import os
import time
import logging
import random
import socket
import threading
import redis

logger = logging.getLogger(__name__)

_shutdown_event = threading.Event()


def request_shutdown():
    """Signal all consume() loops in this process to drain and exit."""
    logger.info("Shutdown requested — draining workers …")
    _shutdown_event.set()


def is_shutting_down() -> bool:
    return _shutdown_event.is_set()


def _make_bus_client() -> redis.Redis:
    sentinel_host = os.environ.get('BUS_REDIS_SENTINEL_HOST')
    if sentinel_host:
        sentinel_port = int(os.environ.get('BUS_REDIS_SENTINEL_PORT', '26379'))
        master_name = os.environ.get('BUS_REDIS_SENTINEL_MASTER', 'mymaster')
        password = os.environ.get('BUS_REDIS_PASSWORD', '')
        db_num = int(os.environ.get('BUS_REDIS_DB', '0'))
        sentinel = redis.Sentinel(
            [(sentinel_host, sentinel_port)],
            sentinel_kwargs={'password': password},
            password=password,
            db=db_num,
            socket_timeout=5,
            retry_on_timeout=True,
        )
        return sentinel.master_for(master_name)
    else:
        return redis.Redis(
            host=os.environ['BUS_REDIS_HOST'],
            port=int(os.environ['BUS_REDIS_PORT']),
            password=os.environ['BUS_REDIS_PASSWORD'],
            db=int(os.environ['BUS_REDIS_DB']),
            socket_timeout=5,
            retry_on_timeout=True,
        )


bus_db = _make_bus_client()

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


PEL_IDLE_MS = 30_000          # reclaim messages idle longer than 30 s
PEL_CHECK_INTERVAL = 30.0     # seconds between reclaim sweeps


def consume(service: str, worker_id: str, batch: int = 50):
    stream = STREAMS[service]
    group  = GROUPS[service]
    last_reclaim = time.time() - random.uniform(0, PEL_CHECK_INTERVAL)

    while not _shutdown_event.is_set():
        # --- Periodically reclaim orphaned PEL messages ---
        now = time.time()
        if now - last_reclaim >= PEL_CHECK_INTERVAL:
            last_reclaim = now
            try:
                _, claimed, _ = bus_db.xautoclaim(
                    name=stream,
                    groupname=group,
                    consumername=worker_id,
                    min_idle_time=PEL_IDLE_MS,
                    start_id="0-0",
                    count=batch,
                )
                if claimed:
                    logger.info(
                        f"[{service}] Reclaimed {len(claimed)} orphaned PEL message(s)"
                    )
                    for msg_id, data in claimed:
                        if data:
                            yield msg_id, data
            except redis.exceptions.RedisError as e:
                logger.warning(f"[{service}] PEL reclaim error: {e}")

        # --- Read new messages ---
        try:
            results = bus_db.xreadgroup(
                groupname=group,
                consumername=worker_id,
                streams={stream: ">"},
                count=batch,
                block=50,
            )
            if results:
                for _, messages in results:
                    for msg_id, data in messages:
                        yield msg_id, data
        except redis.exceptions.RedisError as e:
            logger.error(f"[{service}] Redis error in consume: {e}")
            time.sleep(0.1)

    logger.info(f"[{service}] Worker {worker_id} exiting gracefully")


def ack(service: str, msg_id):
    bus_db.xack(STREAMS[service], GROUPS[service], msg_id)
