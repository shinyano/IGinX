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
        description="Aggregate and plot IGinX adaptive UDF architecture benchmark results."
    )
    parser.add_argument("--throughput-csv", required=True)
    parser.add_argument("--latency-csv", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument(
        "--title-prefix",
        default="IGinX Adaptive Python UDF Architecture Benchmark",
    )
    return parser.parse_args()


def mean(values):
    return sum(values) / len(values) if values else 0.0


def stddev(values):
    if len(values) <= 1:
        return 0.0
    avg = mean(values)
    variance = sum((value - avg) ** 2 for value in values) / (len(values) - 1)
    return math.sqrt(variance)


def percentile(values, p):
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * p
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def read_throughput_rows(path):
    rows = []
    with open(path, "r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if row.get("status") != "ok":
                continue
            rows.append(
                {
                    "architecture": row["architecture"],
                    "round": int(row["round"]),
                    "scenario": row["scenario"],
                    "concurrency": int(row["concurrency"]),
                    "throughput": float(row["throughput"]),
                }
            )
    return rows


def read_latency_rows(path):
    rows = []
    with open(path, "r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            rows.append(
                {
                    "architecture": row["architecture"],
                    "round": int(row["round"]),
                    "scenario": row["scenario"],
                    "workloadType": row["workloadType"],
                    "concurrency": int(row["concurrency"]),
                    "latencyMs": float(row["latencyMs"]),
                }
            )
    return rows


def aggregate_throughput(rows):
    grouped = defaultdict(list)
    for row in rows:
        grouped[(row["architecture"], row["scenario"], row["concurrency"])].append(
            row["throughput"]
        )

    result = {}
    for key, values in grouped.items():
        result[key] = {
            "throughputMean": mean(values),
            "throughputStd": stddev(values),
            "samples": len(values),
        }
    return result


def aggregate_latency(rows):
    per_round = defaultdict(list)
    for row in rows:
        key = (
            row["architecture"],
            row["scenario"],
            row["workloadType"],
            row["concurrency"],
            row["round"],
        )
        per_round[key].append(row["latencyMs"])

    percentiles_by_point = defaultdict(lambda: {"p50": [], "p95": [], "p99": []})
    for key, latencies in per_round.items():
        point_key = key[:-1]
        percentiles_by_point[point_key]["p50"].append(percentile(latencies, 0.50))
        percentiles_by_point[point_key]["p95"].append(percentile(latencies, 0.95))
        percentiles_by_point[point_key]["p99"].append(percentile(latencies, 0.99))

    result = {}
    for key, values in percentiles_by_point.items():
        result[key] = {
            "p50": mean(values["p50"]),
            "p95": mean(values["p95"]),
            "p99": mean(values["p99"]),
        }
    return result


def build_summary(throughput_summary, latency_summary):
    combined = {}
    for key, value in throughput_summary.items():
        architecture, scenario, concurrency = key
        combined[(architecture, scenario, concurrency)] = {
            "architecture": architecture,
            "scenario": scenario,
            "concurrency": concurrency,
            "throughputMean": value["throughputMean"],
            "throughputStd": value["throughputStd"],
            "udfP50": 0.0,
            "udfP95": 0.0,
            "udfP99": 0.0,
            "lightP50": 0.0,
            "lightP95": 0.0,
            "lightP99": 0.0,
        }

    for key, value in latency_summary.items():
        architecture, scenario, workload_type, concurrency = key
        row = combined.setdefault(
            (architecture, scenario, concurrency),
            {
                "architecture": architecture,
                "scenario": scenario,
                "concurrency": concurrency,
                "throughputMean": 0.0,
                "throughputStd": 0.0,
                "udfP50": 0.0,
                "udfP95": 0.0,
                "udfP99": 0.0,
                "lightP50": 0.0,
                "lightP95": 0.0,
                "lightP99": 0.0,
            },
        )
        prefix = "udf" if workload_type == "udf" else "light"
        row[f"{prefix}P50"] = value["p50"]
        row[f"{prefix}P95"] = value["p95"]
        row[f"{prefix}P99"] = value["p99"]

    return [
        combined[key]
        for key in sorted(
            combined.keys(), key=lambda item: (item[1], item[0], item[2])
        )
    ]


def write_summary(output_dir, summary_rows):
    output_dir.mkdir(parents=True, exist_ok=True)
    summary_path = output_dir / "summary.csv"
    with open(summary_path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "architecture",
                "scenario",
                "concurrency",
                "throughputMean",
                "throughputStd",
                "udfP50",
                "udfP95",
                "udfP99",
                "lightP50",
                "lightP95",
                "lightP99",
            ],
        )
        writer.writeheader()
        for row in summary_rows:
            writer.writerow(
                {
                    "architecture": row["architecture"],
                    "scenario": row["scenario"],
                    "concurrency": row["concurrency"],
                    "throughputMean": f"{row['throughputMean']:.4f}",
                    "throughputStd": f"{row['throughputStd']:.4f}",
                    "udfP50": f"{row['udfP50']:.4f}",
                    "udfP95": f"{row['udfP95']:.4f}",
                    "udfP99": f"{row['udfP99']:.4f}",
                    "lightP50": f"{row['lightP50']:.4f}",
                    "lightP95": f"{row['lightP95']:.4f}",
                    "lightP99": f"{row['lightP99']:.4f}",
                }
            )
    return summary_path


def plot(summary_rows, output_dir, title_prefix):
    try:
        import matplotlib.pyplot as plt
    except ImportError as exc:
        raise RuntimeError("matplotlib is required for plotting") from exc

    steady_rows = [row for row in summary_rows if row["scenario"] == "steady_udf"]
    mixed_rows = [row for row in summary_rows if row["scenario"] == "mixed_projection"]

    throughput_fig = plt.figure(figsize=(9, 5.5))
    for architecture in sorted({row["architecture"] for row in steady_rows}):
        rows = sorted(
            [row for row in steady_rows if row["architecture"] == architecture],
            key=lambda item: item["concurrency"],
        )
        xs = [row["concurrency"] for row in rows]
        ys = [row["throughputMean"] for row in rows]
        plt.plot(xs, ys, marker="o", linewidth=2, label=architecture)
    plt.xlabel("Concurrency")
    plt.ylabel("Throughput (queries/s)")
    plt.title(f"{title_prefix} - Throughput")
    plt.xticks(sorted({row["concurrency"] for row in steady_rows}))
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.legend()
    plt.tight_layout()
    throughput_path = output_dir / "throughput_vs_concurrency.png"
    throughput_fig.savefig(throughput_path, dpi=200)
    plt.close(throughput_fig)

    udf_latency_fig = plt.figure(figsize=(10, 6))
    line_styles = {"udfP50": "-", "udfP95": "--", "udfP99": ":"}
    for architecture in sorted({row["architecture"] for row in steady_rows}):
        rows = sorted(
            [row for row in steady_rows if row["architecture"] == architecture],
            key=lambda item: item["concurrency"],
        )
        xs = [row["concurrency"] for row in rows]
        for metric, style in line_styles.items():
            ys = [row[metric] for row in rows]
            plt.plot(xs, ys, marker="o", linewidth=2, linestyle=style, label=f"{architecture}-{metric}")
    plt.xlabel("Concurrency")
    plt.ylabel("Latency (ms)")
    plt.title(f"{title_prefix} - UDF Latency Percentiles")
    plt.xticks(sorted({row["concurrency"] for row in steady_rows}))
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.legend()
    plt.tight_layout()
    udf_latency_path = output_dir / "udf_latency_percentiles.png"
    udf_latency_fig.savefig(udf_latency_path, dpi=200)
    plt.close(udf_latency_fig)

    light_fig = plt.figure(figsize=(9, 5.5))
    for architecture in sorted({row["architecture"] for row in mixed_rows}):
        rows = sorted(
            [row for row in mixed_rows if row["architecture"] == architecture],
            key=lambda item: item["concurrency"],
        )
        xs = [row["concurrency"] for row in rows]
        ys = [row["lightP95"] for row in rows]
        plt.plot(xs, ys, marker="o", linewidth=2, label=architecture)
    plt.xlabel("Concurrency")
    plt.ylabel("Light Query P95 (ms)")
    plt.title(f"{title_prefix} - Light Query P95")
    plt.xticks(sorted({row["concurrency"] for row in mixed_rows}))
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.legend()
    plt.tight_layout()
    light_path = output_dir / "light_query_p95_vs_concurrency.png"
    light_fig.savefig(light_path, dpi=200)
    plt.close(light_fig)

    return throughput_path, udf_latency_path, light_path


def main():
    args = parse_args()
    output_dir = Path(args.output_dir).resolve()
    throughput_rows = read_throughput_rows(args.throughput_csv)
    latency_rows = read_latency_rows(args.latency_csv)

    if not throughput_rows:
        raise RuntimeError("no successful throughput rows were found")
    if not latency_rows:
        raise RuntimeError("no latency rows were found")

    throughput_summary = aggregate_throughput(throughput_rows)
    latency_summary = aggregate_latency(latency_rows)
    summary_rows = build_summary(throughput_summary, latency_summary)
    summary_path = write_summary(output_dir, summary_rows)
    throughput_path, udf_latency_path, light_path = plot(
        summary_rows, output_dir, args.title_prefix
    )

    print(f"summary csv: {summary_path}")
    print(f"throughput plot: {throughput_path}")
    print(f"udf latency plot: {udf_latency_path}")
    print(f"light query p95 plot: {light_path}")


if __name__ == "__main__":
    main()
