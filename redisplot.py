import pandas as pd
import matplotlib.pyplot as plt
import sys

csv_file = sys.argv[1] if len(sys.argv) > 1 else "redis_stats.csv"
df = pd.read_csv(csv_file)

t0 = df["timestamp"].min()
df["elapsed_s"] = df["timestamp"] - t0

# Numeric conversions
for col in ["ops_per_sec", "connected_clients", "used_memory_mb", "total_commands",
            "cmd_get", "cmd_set", "cmd_xadd", "cmd_xreadgroup", "cmd_xack",
            "cmd_watch", "cmd_multi", "cmd_exec", "cmd_eval"]:
    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

instances = sorted(df["instance"].unique())
n_instances = len(instances)

# Compute per-interval rates per instance
cmd_cols = ["cmd_get", "cmd_set", "cmd_xadd", "cmd_xreadgroup", "cmd_xack",
            "cmd_watch", "cmd_multi", "cmd_exec", "cmd_eval"]

dfs = []
for inst in instances:
    idf = df[df["instance"] == inst].copy().sort_values("elapsed_s")
    interval = idf["elapsed_s"].diff().median()
    if interval and interval > 0:
        for col in cmd_cols:
            idf[f"{col}_rate"] = idf[col].diff() / interval
    else:
        for col in cmd_cols:
            idf[f"{col}_rate"] = 0
    dfs.append(idf.iloc[1:])  # drop first row (no diff)

df = pd.concat(dfs, ignore_index=True)

# ── Plot 1: Ops/sec per instance ─────────────────────────────────────────────
fig, axes = plt.subplots(4, 1, figsize=(16, 20), sharex=True)

ax = axes[0]
for inst in instances:
    idf = df[df["instance"] == inst]
    ax.plot(idf["elapsed_s"], idf["ops_per_sec"], label=inst, linewidth=1.5)
ax.set_ylabel("Ops/sec")
ax.set_title("Redis Operations Per Second (per instance)")
ax.legend(loc="upper left", fontsize=8)
ax.grid(True, alpha=0.3)

# ── Plot 2: Data command rates per instance ──────────────────────────────────
ax = axes[1]
colors = plt.cm.tab10.colors
for i, inst in enumerate(instances):
    idf = df[df["instance"] == inst]
    # Sum data commands: GET + SET + WATCH + MULTI + EXEC + EVAL
    data_rate = (idf["cmd_get_rate"] + idf["cmd_set_rate"] +
                 idf["cmd_watch_rate"] + idf["cmd_multi_rate"] +
                 idf["cmd_exec_rate"] + idf["cmd_eval_rate"])
    ax.plot(idf["elapsed_s"], data_rate, label=f"{inst} (data)", linewidth=1.5, color=colors[i % len(colors)])
ax.set_ylabel("Cmds/sec")
ax.set_title("Data Commands Per Second (GET+SET+WATCH+MULTI+EXEC+EVAL per instance)")
ax.legend(loc="upper left", fontsize=8)
ax.grid(True, alpha=0.3)

# ── Plot 3: Stream command rates per instance ────────────────────────────────
ax = axes[2]
for i, inst in enumerate(instances):
    idf = df[df["instance"] == inst]
    stream_rate = idf["cmd_xadd_rate"] + idf["cmd_xreadgroup_rate"] + idf["cmd_xack_rate"]
    ax.plot(idf["elapsed_s"], stream_rate, label=f"{inst} (streams)", linewidth=1.5, color=colors[i % len(colors)])
ax.set_ylabel("Cmds/sec")
ax.set_title("Stream Commands Per Second (XADD+XREADGROUP+XACK per instance)")
ax.legend(loc="upper left", fontsize=8)
ax.grid(True, alpha=0.3)

# ── Plot 4: Memory per instance ──────────────────────────────────────────────
ax = axes[3]
for inst in instances:
    idf = df[df["instance"] == inst]
    ax.plot(idf["elapsed_s"], idf["used_memory_mb"], label=inst, linewidth=1.5)
ax.set_ylabel("Memory (MiB)")
ax.set_xlabel("Elapsed Time (seconds)")
ax.set_title("Redis Memory Usage (per instance)")
ax.legend(loc="upper left", fontsize=8)
ax.grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig("redis_stats.png", dpi=150, bbox_inches="tight")
print("Saved redis_stats.png")

# ── Detailed breakdown per instance ──────────────────────────────────────────
if n_instances > 1:
    fig, axes = plt.subplots(n_instances, 2, figsize=(16, 5 * n_instances), sharex=True)
    if n_instances == 1:
        axes = [axes]

    for i, inst in enumerate(instances):
        idf = df[df["instance"] == inst]

        # Data commands breakdown
        ax = axes[i][0]
        for col, label in [("cmd_get_rate", "GET"), ("cmd_set_rate", "SET"),
                           ("cmd_watch_rate", "WATCH"), ("cmd_multi_rate", "MULTI"),
                           ("cmd_exec_rate", "EXEC"), ("cmd_eval_rate", "EVAL")]:
            ax.plot(idf["elapsed_s"], idf[col], label=label, linewidth=1)
        ax.set_ylabel("Cmds/sec")
        ax.set_title(f"{inst} — Data Commands")
        ax.legend(fontsize=7)
        ax.grid(True, alpha=0.3)

        # Stream commands breakdown
        ax = axes[i][1]
        for col, label in [("cmd_xadd_rate", "XADD"),
                           ("cmd_xreadgroup_rate", "XREADGROUP"),
                           ("cmd_xack_rate", "XACK")]:
            ax.plot(idf["elapsed_s"], idf[col], label=label, linewidth=1)
        ax.set_ylabel("Cmds/sec")
        ax.set_title(f"{inst} — Stream Commands")
        ax.legend(fontsize=7)
        ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig("redis_stats_detailed.png", dpi=150, bbox_inches="tight")
    print("Saved redis_stats_detailed.png")

# ── Summary ──────────────────────────────────────────────────────────────────
print(f"\n=== Redis Summary ===")
for inst in instances:
    idf = df[df["instance"] == inst]
    total_interval = idf["elapsed_s"].iloc[-1] - idf["elapsed_s"].iloc[0] if len(idf) > 1 else 0
    print(f"\n--- {inst} ({total_interval:.0f}s) ---")
    print(f"  Avg ops/sec:     {idf['ops_per_sec'].mean():.0f}")
    print(f"  Max ops/sec:     {idf['ops_per_sec'].max():.0f}")
    print(f"  Avg clients:     {idf['connected_clients'].mean():.0f}")
    print(f"  Max memory (MB): {idf['used_memory_mb'].max():.1f}")

    total_rate = idf["ops_per_sec"].mean()
    if total_rate > 0:
        print(f"  Command breakdown:")
        for col in cmd_cols:
            rate = idf[f"{col}_rate"].mean()
            if rate > 0:
                name = col.replace("cmd_", "").upper()
                pct = rate / total_rate * 100
                print(f"    {name:15s}: {rate:>8,.0f}  ({pct:.1f}%)")