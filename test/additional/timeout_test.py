import os
import time
import requests
import subprocess
from verify_consistency import verify
from reset_state import reset

reset()

BASE = "http://localhost:5000"
PROJECT_NAME = "dds26-team9"


def run(cmd):
    return subprocess.run(cmd, capture_output=True, text=True, check=False)


def get_container_id(service_name: str) -> str:
    # Works for docker compose services by label
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


def stop_container(container_id: str):
    result = run(["docker", "stop", container_id])
    if result.returncode != 0:
        raise RuntimeError(f"Failed to stop container {container_id}: {result.stderr.strip()}")


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
    raise RuntimeError(f"Container {container_id} did not become running again in time")


def get_order(order_id: str):
    r = requests.get(f"{BASE}/orders/find/{order_id}", timeout=5)
    r.raise_for_status()
    return r.json()

PAYMENT_SERVICE = "user-worker"

payment_container = get_container_id(PAYMENT_SERVICE)

print(f"Stopping payment service container: {payment_container}")
stop_container(payment_container)

if is_running(payment_container):
    raise RuntimeError("Payment container is still running after stop()")

order = requests.post(f"{BASE}/orders/create/0", timeout=5).json()["order_id"]
requests.post(f"{BASE}/orders/addItem/{order}/0/1", timeout=5)

checkout_status = None
checkout_error = None

try:
    r = requests.post(f"{BASE}/orders/checkout/{order}", timeout=15)
    checkout_status = r.status_code
    print("Checkout response:", checkout_status)
except requests.RequestException as e:
    checkout_error = str(e)
    print("Checkout raised exception:", checkout_error)

print(f"Restarting payment service container: {payment_container}")
start_container(payment_container)
wait_until_running(payment_container)

# Give the system a few seconds to replay/recover in-flight work
time.sleep(5)

order_state = get_order(order)
print("Final order state:", order_state)

if verify():
    print("TIMEOUT TEST PASS")
else:
    print("TIMEOUT TEST FAIL")