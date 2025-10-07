import json
import os
import matplotlib.pyplot as plt
import numpy as np

BENCH_DIR = ".benchmarks/Linux-CPython-3.13-64bit"
OUT_DIR = "datamarts"
REPORT_PATH = os.path.join(OUT_DIR, "benchmark_summary.txt")
os.makedirs(OUT_DIR, exist_ok=True)

def load_latest():
    files = sorted(
        [os.path.join(BENCH_DIR, f) for f in os.listdir(BENCH_DIR)],
        key=os.path.getmtime, reverse=True
    )
    if not files:
        raise RuntimeError("No benchmark files found.")
    latest = files[0]
    print(f"📊 Loading: {latest}")
    with open(latest) as f:
        return json.load(f)

def extract(data):
    res = []
    for b in data["benchmarks"]:
        res.append((b.get("group",""), b["name"], b["stats"]["mean"]*1000))
    return res

def plot_group(results, group, title):
    group_data = [r for r in results if r[0].startswith(group)]
    if not group_data: return
    labels = [r[1].replace("test_","") for r in group_data]
    values = [r[2] for r in group_data]
    plt.figure(figsize=(10,6))
    bars = plt.bar(labels, values, color="#4e9", alpha=0.8, edgecolor="black")
    for bar in bars:
        h = bar.get_height()
        plt.text(bar.get_x()+bar.get_width()/2, h+1, f"{h:.1f} ms", ha="center", va="bottom", fontsize=8)
    plt.title(title)
    plt.ylabel("Średni czas [ms]")
    plt.xticks(rotation=30, ha="right")
    plt.grid(axis="y", alpha=0.3)
    path = os.path.join(OUT_DIR, f"{group}_comparison.png")
    plt.tight_layout()
    plt.savefig(path)
    plt.close()
    print(f"✅ {path}")

def summary(results):
    groups = {}
    for g,n,t in results:
        groups.setdefault(g, []).append((n,t))
    with open(REPORT_PATH,"w") as f:
        f.write("=== BENCHMARK SUMMARY ===\n\n")
        for g,data in groups.items():
            avg = np.mean([t for _,t in data])
            fastest = min(data, key=lambda x:x[1])
            slowest = max(data, key=lambda x:x[1])
            diff = ((slowest[1]-fastest[1])/fastest[1])*100
            f.write(f"[{g}]\n")
            f.write(f"  Avg time: {avg:.1f} ms\n")
            f.write(f"  Fastest: {fastest[0]} ({fastest[1]:.1f} ms)\n")
            f.write(f"  Slowest: {slowest[0]} ({slowest[1]:.1f} ms)\n")
            f.write(f"  Δ Slowest/Fastest: {diff:.1f}%\n\n")
    print(f"📝 Summary saved → {REPORT_PATH}")

def main():
    data = load_latest()
    results = extract(data)
    plot_group(results,"index_build","Czas budowy indeksów")
    plot_group(results,"query_single","Pojedyncze zapytania")
    plot_group(results,"query_multi","Wielowyrazowe zapytania")
    plot_group(results,"bulk_upsert","Wstawianie metadanych (bulk upsert)")
    plot_group(results,"query","Zapytania po autorze")
    summary(results)

if __name__ == "__main__":
    main()
