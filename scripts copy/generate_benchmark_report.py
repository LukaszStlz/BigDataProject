import json
import os
import matplotlib.pyplot as plt

BENCH_DIR = ".benchmarks/Linux-CPython-3.13-64bit"
OUT_DIR = "datamarts"
os.makedirs(OUT_DIR, exist_ok=True)

def load_latest_benchmark():
    files = sorted(
        [os.path.join(BENCH_DIR, f) for f in os.listdir(BENCH_DIR)],
        key=os.path.getmtime,
        reverse=True
    )
    if not files:
        raise RuntimeError(f"No benchmark files found in {BENCH_DIR}")
    with open(files[0], "r") as f:
        return json.load(f)

def extract_results(data):
    results = []
    for bench in data["benchmarks"]:
        name = bench["name"]
        group = bench.get("group", "")
        mean = bench["stats"]["mean"] * 1000  # sekundy → ms
        results.append((group, name, mean))
    return results

def plot_group(results, group_name, title):
    group = [r for r in results if r[0] == group_name]
    if not group:
        return
    labels = [r[1].replace("test_", "").replace("_bench", "") for r in group]
    values = [r[2] for r in group]

    plt.figure(figsize=(10, 6))
    plt.bar(labels, values, color="skyblue")
    plt.title(title)
    plt.ylabel("Średni czas [ms]")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    path = os.path.join(OUT_DIR, f"{group_name}_comparison.png")
    plt.savefig(path)
    print(f"✅ Saved plot: {path}")

if __name__ == "__main__":
    data = load_latest_benchmark()
    results = extract_results(data)
    plot_group(results, "bulk_upsert", "Porównanie prędkości zapisu (Bulk Upsert)")
    plot_group(results, "query", "Porównanie prędkości zapytań (Query by Author)")
    plot_group(results, "index_build", "Czas budowy indeksu odwróconego")
