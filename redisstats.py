#!/usr/bin/env python3
"""Redis stats collector for split-Redis architecture.

Usage:
    # Single Redis (old architecture):
    python3 redisstats.py redis-master-1

    # Split Redis (new architecture) — pass all containers:
    python3 redisstats.py redis-bus-1 redis-order-1 redis-stock-1 redis-payment-1 redis-orchestrator-1

    # Auto-discover all Redis containers:
    python3 redisstats.py --auto
"""

import subprocess
import sys
import time
import csv
import re

INTERVAL = 2
OUTFILE = "redis_stats.csv"

FIELDS = [
    "timestamp", "instance", "ops_per_sec", "connected_clients", "used_memory_mb",
    "cmd_get", "cmd_set", "cmd_xadd", "cmd_xreadgroup", "cmd_xack",
    "cmd_watch", "cmd_multi", "cmd_exec", "cmd_eval",
    "total_commands",
]

INFO_KEYS = {
    "instantaneous_ops_per_sec": "ops_per_sec",
    "connected_clients": "connected_clients",
    "used_memory": "used_memory_raw",
    "total_commands_processed": "total_commands",
}

CMD_KEYS = {
    "get": "cmd_get",
    "set": "cmd_set",
    "xadd": "cmd_xadd",
    "xreadgroup": "cmd_xreadgroup",
    "xack": "cmd_xack",
    "watch": "cmd_watch",
    "multi": "cmd_multi",
    "exec": "cmd_exec",
    "evalsha": "cmd_eval",
    "eval": "cmd_eval_fallback",
}


def discover_redis_containers():
    """Find all running containers with 'redis' in the name."""
    try:
        result = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}"],
            capture_output=True, text=True, timeout=5,
        )
        names = [n.strip() for n in result.stdout.splitlines() if "redis" in n.lower()]
        # Exclude sentinels
        names = [n for n in names if "sentinel" not in n.lower()]
        return sorted(names)
    except Exception as e:
        print(f"Error discovering containers: {e}", file=sys.stderr)
        return []


def get_redis_info(container):
    try:
        result = subprocess.run(
            ["docker", "exec", container, "redis-cli", "-a", "redis", "INFO", "ALL"],
            capture_output=True, text=True, timeout=5,
        )
        return result.stdout
    except Exception as e:
        return ""


def parse_info(raw):
    data = {}
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if ":" not in line:
            continue
        key, _, value = line.partition(":")

        if key in INFO_KEYS:
            data[INFO_KEYS[key]] = value.strip()

        if key.startswith("cmdstat_"):
            cmd_name = key[len("cmdstat_"):]
            if cmd_name in CMD_KEYS:
                match = re.search(r"calls=(\d+)", value)
                if match:
                    data[CMD_KEYS[cmd_name]] = match.group(1)
    return data


def instance_label(container_name):
    """Extract a short label like 'redis-bus' from 'dds26-team9-redis-bus-1'."""
    # Remove common project prefixes and trailing -1
    name = container_name
    for prefix in ["dds26-team9-", "dds26_team9-"]:
        if name.startswith(prefix):
            name = name[len(prefix):]
    # Remove trailing replica number
    name = re.sub(r"-\d+$", "", name)
    return name


def main():
    # Determine containers
    if len(sys.argv) > 1 and sys.argv[1] == "--auto":
        containers = discover_redis_containers()
        if not containers:
            print("No Redis containers found!", file=sys.stderr)
            sys.exit(1)
        print(f"Auto-discovered: {containers}")
    elif len(sys.argv) > 1:
        containers = sys.argv[1:]
    else:
        containers = discover_redis_containers()
        if not containers:
            containers = ["dds26-team9-redis-master-1"]
        print(f"Using: {containers}")

    with open(OUTFILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        f.flush()

        print(f"Collecting stats from {len(containers)} Redis instance(s) every {INTERVAL}s → {OUTFILE}")
        print("Press Ctrl+C to stop.\n")

        try:
            while True:
                ts = int(time.time())

                for container in containers:
                    raw = get_redis_info(container)
                    if not raw:
                        continue

                    parsed = parse_info(raw)
                    label = instance_label(container)

                    row = {
                        "timestamp": ts,
                        "instance": label,
                        "ops_per_sec": parsed.get("ops_per_sec", "0"),
                        "connected_clients": parsed.get("connected_clients", "0"),
                        "total_commands": parsed.get("total_commands", "0"),
                    }

                    mem_raw = parsed.get("used_memory_raw", "0")
                    try:
                        row["used_memory_mb"] = f"{int(mem_raw) / 1048576:.1f}"
                    except ValueError:
                        row["used_memory_mb"] = "0"

                    for cmd, field in CMD_KEYS.items():
                        if field == "cmd_eval_fallback":
                            if row.get("cmd_eval", "0") == "0":
                                row["cmd_eval"] = parsed.get("cmd_eval_fallback", "0")
                        elif field in FIELDS:
                            row[field] = parsed.get(field, "0")

                    writer.writerow(row)

                    print(f"  [{label}] ops={row['ops_per_sec']} "
                          f"clients={row['connected_clients']} "
                          f"mem={row['used_memory_mb']}MB", flush=True)

                f.flush()
                print(f"--- {ts} ---", flush=True)
                time.sleep(INTERVAL)

        except KeyboardInterrupt:
            print(f"\nStopped. Data saved to {OUTFILE}")


if __name__ == "__main__":
    main()