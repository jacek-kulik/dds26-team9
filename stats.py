import pandas as pd
import matplotlib.pyplot as plt
import sys
import re

csv_file = sys.argv[1] if len(sys.argv) > 1 else "stats.csv"

# If your CSV has no header, uncomment this block and remove the plain read_csv below:
# df = pd.read_csv(
#     csv_file,
#     header=None,
#     names=["timestamp", "container", "cpu", "mem_usage", "net_io"]
# )

df = pd.read_csv(csv_file)

# Parse CPU percentage
df["cpu"] = df["cpu"].str.rstrip("%").astype(float)

# Parse memory usage (e.g. "123.4MiB / 512MiB" -> MiB used)
def parse_mem(val):
    used = str(val).split("/")[0].strip()
    match = re.match(r"([\d.]+)([A-Za-z]+)", used)
    if not match:
        return 0.0

    num = float(match.group(1))
    unit = match.group(2)

    if unit == "GiB":
        return num * 1024
    elif unit == "MiB":
        return num
    elif unit == "KiB":
        return num / 1024
    elif unit == "B":
        return num / (1024 * 1024)
    else:
        return 0.0

df["mem_mb"] = df["mem_usage"].apply(parse_mem)

# Convert timestamp to datetime / elapsed seconds
df["time"] = pd.to_datetime(df["timestamp"], unit="s")
t0 = df["time"].min()
df["elapsed_s"] = (df["time"] - t0).dt.total_seconds()

# Group containers by service type
def service_group(name):
    known_services = [
        "redis-master",
        "redis-replica",
        "redis-sentinel",
        "gateway",
        "orchestrator-worker",
        "order-web",
        "order-worker",
        "stock-web",
        "stock-worker",
        "user-web",
        "user-worker",
    ]
    for svc in known_services:
        if svc in name:
            return svc
    return name

df["service"] = df["container"].apply(service_group)

# Aggregate by timestamp and service, keeping occurrence count
agg = (
    df.groupby(["elapsed_s", "service"])
      .agg(
          occurrences=("container", "count"),
          cpu_total=("cpu", "sum"),
          cpu_avg=("cpu", "mean"),
          mem_total=("mem_mb", "sum"),
          mem_avg=("mem_mb", "mean"),
      )
      .reset_index()
)

# ── Plot: service-only, occurrence-aware (average per replica) ───────────────
fig, axes = plt.subplots(3, 1, figsize=(14, 14), sharex=True)

# CPU average per occurrence
ax = axes[0]
for svc, grp in agg.groupby("service"):
    grp = grp.sort_values("elapsed_s")
    ax.plot(grp["elapsed_s"], grp["cpu_avg"], label=svc, linewidth=1.8)

ax.set_ylabel("CPU % per replica")
ax.set_title("CPU Usage by Service (average across occurrences)")
ax.legend(bbox_to_anchor=(1.02, 1), loc="upper left", fontsize=8)
ax.grid(True, alpha=0.3)

# Memory average per occurrence
ax = axes[1]
for svc, grp in agg.groupby("service"):
    grp = grp.sort_values("elapsed_s")
    ax.plot(grp["elapsed_s"], grp["mem_avg"], label=svc, linewidth=1.8)

ax.set_ylabel("Memory (MiB) per replica")
ax.set_title("Memory Usage by Service (average across occurrences)")
ax.legend(bbox_to_anchor=(1.02, 1), loc="upper left", fontsize=8)
ax.grid(True, alpha=0.3)

# Number of occurrences
ax = axes[2]
for svc, grp in agg.groupby("service"):
    grp = grp.sort_values("elapsed_s")
    ax.plot(grp["elapsed_s"], grp["occurrences"], label=svc, linewidth=1.8)

ax.set_ylabel("Occurrences")
ax.set_xlabel("Elapsed Time (seconds)")
ax.set_title("Number of Occurrences per Service Over Time")
ax.legend(bbox_to_anchor=(1.02, 1), loc="upper left", fontsize=8)
ax.grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig("per_service_occurrence_aware.png", dpi=150, bbox_inches="tight")
print("Saved per_service_occurrence_aware.png")

# ── Optional extra plot: total resource usage per service ─────────────────────
fig, axes = plt.subplots(2, 1, figsize=(14, 10), sharex=True)

ax = axes[0]
for svc, grp in agg.groupby("service"):
    grp = grp.sort_values("elapsed_s")
    ax.plot(grp["elapsed_s"], grp["cpu_total"], label=svc, linewidth=1.8)

ax.set_ylabel("Total CPU %")
ax.set_title("Total CPU Usage by Service (sum across occurrences)")
ax.legend(bbox_to_anchor=(1.02, 1), loc="upper left", fontsize=8)
ax.grid(True, alpha=0.3)

ax = axes[1]
for svc, grp in agg.groupby("service"):
    grp = grp.sort_values("elapsed_s")
    ax.plot(grp["elapsed_s"], grp["mem_total"], label=svc, linewidth=1.8)

ax.set_ylabel("Total Memory (MiB)")
ax.set_xlabel("Elapsed Time (seconds)")
ax.set_title("Total Memory Usage by Service (sum across occurrences)")
ax.legend(bbox_to_anchor=(1.02, 1), loc="upper left", fontsize=8)
ax.grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig("per_service_total.png", dpi=150, bbox_inches="tight")
print("Saved per_service_total.png")

# ── Summary table ─────────────────────────────────────────────────────────────
summary = (
    agg.groupby("service")
       .agg(
           avg_occurrences=("occurrences", "mean"),
           max_occurrences=("occurrences", "max"),
           cpu_avg_per_replica=("cpu_avg", "mean"),
           cpu_peak_per_replica=("cpu_avg", "max"),
           cpu_peak_total=("cpu_total", "max"),
           mem_avg_per_replica=("mem_avg", "mean"),
           mem_peak_per_replica=("mem_avg", "max"),
           mem_peak_total=("mem_total", "max"),
       )
       .round(2)
       .sort_values("cpu_peak_total", ascending=False)
)

print("\n=== Service Summary ===")
print(summary.to_string())