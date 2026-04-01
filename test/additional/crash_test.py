import asyncio
import aiohttp
import time
import requests
import subprocess
from verify_consistency import verify
from reset_state import reset

reset()

BASE = "http://localhost:5000"
PROJECT_NAME = "dds26-team9"
NUM_WORKERS = 50
TEST_DURATION = 20
KILL_AT = 5
RESTART_AFTER = 5


def run(cmd):
    return subprocess.run(cmd, capture_output=True, text=True, check=False)


def get_container_id(service_name: str) -> str:
    result = run([
        "docker", "ps", "-a", "-q",
        "--filter", f"label=com.docker.compose.project={PROJECT_NAME}",
        "--filter", f"label=com.docker.compose.service={service_name}",
    ])
    cid = result.stdout.strip()
    if not cid:
        raise RuntimeError(f"Could not find container for service '{service_name}'")
    return cid.splitlines()[0]


def is_running(container_id: str) -> bool:
    result = run(["docker", "inspect", "-f", "{{.State.Running}}", container_id])
    return result.stdout.strip() == "true"


def kill_container(container_id: str):
    result = run(["docker", "kill", container_id])
    if result.returncode != 0:
        raise RuntimeError(f"Failed to kill container {container_id}: {result.stderr.strip()}")


def start_container(container_id: str):
    result = run(["docker", "start", container_id])
    if result.returncode != 0:
        raise RuntimeError(f"Failed to start container {container_id}: {result.stderr.strip()}")


def wait_until_running(container_id: str, timeout: float = 20.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if is_running(container_id):
            return
        time.sleep(0.5)
    raise RuntimeError(f"Container {container_id} did not restart in time")


async def checkout_loop(stop_event: asyncio.Event):
    timeout = aiohttp.ClientTimeout(total=10)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        while not stop_event.is_set():
            try:
                async with session.post(f"{BASE}/orders/create/0") as r:
                    if r.status != 200:
                        continue
                    order = (await r.json())["order_id"]

                async with session.post(f"{BASE}/orders/addItem/{order}/0/1") as r:
                    if r.status != 200:
                        continue

                await session.post(f"{BASE}/orders/checkout/{order}")

            except Exception:
                await asyncio.sleep(0.05)


async def crash_controller():
    payment_container = get_container_id("user-worker")

    await asyncio.sleep(KILL_AT)

    print(f"Killing payment service container: {payment_container}")
    kill_container(payment_container)

    if is_running(payment_container):
        raise RuntimeError("Payment container is still running after kill()")

    await asyncio.sleep(RESTART_AFTER)

    print(f"Restarting payment service container: {payment_container}")
    start_container(payment_container)
    wait_until_running(payment_container)


async def main():
    stop_event = asyncio.Event()

    workers = [asyncio.create_task(checkout_loop(stop_event)) for _ in range(NUM_WORKERS)]
    controller = asyncio.create_task(crash_controller())

    await asyncio.sleep(TEST_DURATION)
    stop_event.set()

    await asyncio.gather(*workers, return_exceptions=True)
    await controller

    print("Crash test finished.")

    if verify():
        print("CRASH TEST PASS")
    else:
        print("CRASH TEST FAIL")


if __name__ == "__main__":
    asyncio.run(main())