import json
import os
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path


BENCH_PATH = Path(".benchmarks")
RESULT_FILE = None


for root, _, files in os.walk(BENCH_PATH):
    for f in files:
        if f.endswith(".json") and "stage1_results" in f:
            RESULT_FILE = Path(root) / f

if RESULT_FILE is None:
    raise FileNotFoundError("No benchmark results found! Run pytest with --benchmark-save=stage1_results first.")


PLOTS_DIR = Path("plots")
PLOTS_DIR.mkdir(exist_ok=True)


with open(RESULT_FILE, "r", encoding="utf-8") as f:
    data = json.load(f)

benchmarks = data["benchmarks"]


names = []
means = []
stddevs = []

for bench in benchmarks:
    name = bench["name"]
    stats = bench["stats"]
    names.append(name)
    means.append(stats["mean"])
    stddevs.append(stats["stddev"])


indexing_names = [n for n in names if "indexing" in n]
search_names = [n for n in names if "search" in n]

def group_data(group_names):
    grouped_means = [means[names.index(n)] for n in group_names]
    grouped_stds = [stddevs[names.index(n)] for n in group_names]
    return grouped_means, grouped_stds


if indexing_names:
    idx_means, idx_stds = group_data(indexing_names)
    plt.figure(figsize=(6, 4))
    plt.bar(indexing_names, idx_means, yerr=idx_stds, capsize=5, color=["#1f77b4", "#ff7f0e"])
    plt.ylabel("Mean time (s/op)")
    plt.title("Indexing Speed Comparison (Redis vs PostgreSQL)")
    plt.grid(axis='y', linestyle="--", alpha=0.7)
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "indexing_comparison.png")
    plt.close()


if search_names:
    search_means, search_stds = group_data(search_names)
    plt.figure(figsize=(6, 4))
    plt.bar(search_names, search_means, yerr=search_stds, capsize=5, color=["#2ca02c", "#d62728"])
    plt.ylabel("Mean time (s/op)")
    plt.title("Search Speed Comparison (Redis vs PostgreSQL)")
    plt.grid(axis='y', linestyle="--", alpha=0.7)
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "search_comparison.png")
    plt.close()


plt.figure(figsize=(8, 5))
plt.barh(names, means, xerr=stddevs, color="skyblue")
plt.xlabel("Mean time (s/op)")
plt.title("All Benchmark Results Overview")
plt.grid(axis='x', linestyle="--", alpha=0.7)
plt.tight_layout()
plt.savefig(PLOTS_DIR / "all_benchmarks.png")
plt.close()

print(f" Plots generated successfully in '{PLOTS_DIR}/' folder.")
