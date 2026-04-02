#!/usr/bin/env python3
#
# IGinX - the polystore system with high performance
# Copyright (C) Tsinghua University
# TSIGinX@gmail.com
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 3 of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, write to the Free Software Foundation,
# Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
#

import argparse
import csv
import math
from collections import defaultdict
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(
        description="Aggregate and plot IGinX Python UDF throughput benchmark results."
    )
    parser.add_argument("--input-csv", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--title-prefix", default="IGinX Python UDF Throughput")
    return parser.parse_args()


def read_rows(input_csv):
    rows = []
    with open(input_csv, "r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if row.get("status") != "ok":
                continue
            rows.append(
                {
                    "mode": row["mode"],
                    "round": int(row["round"]),
                    "workload": row["workload"],
                    "threads": int(row["threads"]),
                    "throughput": float(row["throughput"]),
                    "pythonVersion": row.get("pythonVersion", ""),
                    "gilEnabled": row.get("gilEnabled", ""),
                }
            )
    return rows


def mean(values):
    return sum(values) / len(values) if values else 0.0


def stddev(values):
    if len(values) <= 1:
        return 0.0
    avg = mean(values)
    variance = sum((value - avg) ** 2 for value in values) / (len(values) - 1)
    return math.sqrt(variance)


def aggregate(rows):
    grouped = defaultdict(list)
    for row in rows:
        grouped[(row["mode"], row["workload"], row["threads"])].append(row["throughput"])

    summary = []
    baselines = {}
    for (mode, workload, threads), values in sorted(grouped.items()):
        throughput_mean = mean(values)
        throughput_std = stddev(values)
        summary.append(
            {
                "mode": mode,
                "workload": workload,
                "threads": threads,
                "samples": len(values),
                "throughputMean": throughput_mean,
                "throughputStd": throughput_std,
            }
        )
        if threads == 1:
            baselines[(mode, workload)] = throughput_mean

    for row in summary:
        baseline = baselines.get((row["mode"], row["workload"]), 0.0)
        row["speedup"] = row["throughputMean"] / baseline if baseline > 0 else 0.0
    return summary


def write_summary(output_dir, summary):
    output_dir.mkdir(parents=True, exist_ok=True)
    summary_csv = output_dir / "summary.csv"
    with open(summary_csv, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "mode",
                "workload",
                "threads",
                "samples",
                "throughputMean",
                "throughputStd",
                "speedup",
            ],
        )
        writer.writeheader()
        for row in summary:
            writer.writerow(
                {
                    "mode": row["mode"],
                    "workload": row["workload"],
                    "threads": row["threads"],
                    "samples": row["samples"],
                    "throughputMean": f"{row['throughputMean']:.4f}",
                    "throughputStd": f"{row['throughputStd']:.4f}",
                    "speedup": f"{row['speedup']:.4f}",
                }
            )
    return summary_csv


def plot(summary, output_dir, title_prefix):
    try:
        import matplotlib.pyplot as plt
    except ImportError as exc:
        raise RuntimeError("matplotlib is required for plotting") from exc

    series = defaultdict(list)
    for row in summary:
        series[(row["mode"], row["workload"])].append(row)

    label_map = {
        ("gil", "pure_python"): "GIL-PurePython",
        ("ft", "pure_python"): "FT-PurePython",
        ("gil", "numpy_vectorized"): "GIL-Numpy",
        ("ft", "numpy_vectorized"): "FT-Numpy",
    }

    throughput_fig = plt.figure(figsize=(9, 5.5))
    for key, rows in sorted(series.items()):
        rows = sorted(rows, key=lambda item: item["threads"])
        xs = [row["threads"] for row in rows]
        ys = [row["throughputMean"] for row in rows]
        plt.plot(xs, ys, marker="o", linewidth=2, label=label_map.get(key, f"{key[0]}-{key[1]}"))
    plt.xlabel("Threads")
    plt.ylabel("Throughput (ops/s)")
    plt.title(f"{title_prefix} - Throughput")
    plt.xticks(sorted({row["threads"] for row in summary}))
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.legend()
    plt.tight_layout()
    throughput_path = output_dir / "throughput.png"
    throughput_fig.savefig(throughput_path, dpi=200)
    plt.close(throughput_fig)

    speedup_fig = plt.figure(figsize=(9, 5.5))
    for key, rows in sorted(series.items()):
        rows = sorted(rows, key=lambda item: item["threads"])
        xs = [row["threads"] for row in rows]
        ys = [row["speedup"] for row in rows]
        plt.plot(xs, ys, marker="o", linewidth=2, label=label_map.get(key, f"{key[0]}-{key[1]}"))
    plt.xlabel("Threads")
    plt.ylabel("Speedup")
    plt.title(f"{title_prefix} - Speedup")
    plt.xticks(sorted({row["threads"] for row in summary}))
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.legend()
    plt.tight_layout()
    speedup_path = output_dir / "speedup.png"
    speedup_fig.savefig(speedup_path, dpi=200)
    plt.close(speedup_fig)

    return throughput_path, speedup_path


def main():
    args = parse_args()
    output_dir = Path(args.output_dir).resolve()
    rows = read_rows(args.input_csv)
    if not rows:
        raise RuntimeError("no successful benchmark rows were found")

    summary = aggregate(rows)
    summary_csv = write_summary(output_dir, summary)
    throughput_path, speedup_path = plot(summary, output_dir, args.title_prefix)

    print(f"summary csv: {summary_csv}")
    print(f"throughput plot: {throughput_path}")
    print(f"speedup plot: {speedup_path}")


if __name__ == "__main__":
    main()
