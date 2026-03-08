import requests

BASE = "http://localhost:8000"

def reset():

    print("Resetting system state")

    requests.post(f"{BASE}/payment/batch_init/10/1000")
    requests.post(f"{BASE}/stock/batch_init/10/100/10")

    print("Reset complete")