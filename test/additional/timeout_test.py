import requests
import subprocess
from verify_consistency import verify
from reset_state import reset

reset()

BASE = "http://localhost:8000"

print("Stopping payment service")
subprocess.run(["docker", "stop", "dds26-team9-payment-service-1"])

order = requests.post(f"{BASE}/orders/create/0").json()["order_id"]
requests.post(f"{BASE}/orders/addItem/{order}/0/1")

r = requests.post(f"{BASE}/orders/checkout/{order}")

print("Checkout response:", r.status_code)

print("Restarting payment service")
subprocess.run(["docker", "start", "dds26-team9-payment-service-1"])

if verify():
    print("TIMEOUT TEST PASS")
else:
    print("TIMEOUT TEST FAIL")