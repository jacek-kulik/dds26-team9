import subprocess

import requests

BASE = "http://localhost:5000"

def post(url):
    r = requests.post(url)
    if r.status_code != 200:
        raise Exception(f"Reset failed: {url} -> {r.status_code} {r.text}")


def reset():
    print("Resetting system state")

    post(f"{BASE}/payment/reset")
    post(f"{BASE}/stock/reset")
    post(f"{BASE}/orders/reset")

    # reinitialize clean data
    requests.post(f"{BASE}/payment/batch_init/10/1000")
    requests.post(f"{BASE}/stock/batch_init/10/100/10")

    print("Reset complete")