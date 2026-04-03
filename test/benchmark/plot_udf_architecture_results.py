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
import re
from bisect import bisect_right
from collections import defaultdict
from pathlib import Path


SCHEDULER_PREFIX = "UDF_ADAPTIVE_METRICS "
ROUND_PATTERN = re.compile(r"r(\d+)")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Aggregate and plot IGinX adaptive UDF dynamic benchmark results."
    )
    parser.add_argument("--timeseries-csv", required=True)
    parser.add_argument("--log-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument(
        "--title-prefix",
        default="IGinX Adaptive Python UDF Dynamic Benchmark",
    )
    return parser.parse_args()


def mean(values):
    return sum(values) / len(values) if values else 0.0


def read_timeseries_rows(path):
    rows = []
    with open(path, "r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if row.get("status") != "ok":
                continue
            rows.append(
                {
                    "round": int(row["round"]),
                    "timestampSec": int(row["timestampSec"]),
                    "sampleTimeEpochMs": int(row["sampleTimeEpochMs"]),
                    "experimentStartEpochMs": int(row["experimentStartEpochMs"]),
                    "phase": row["phase"],
                    "targetConcurrency": int(row["targetConcurrency"]),
                    "completedRequests": int(row["completedRequests"]),
                    "throughputPerSecond": float(row["throughputPerSecond"]),
                    "udfP50": float(row["udfP50"]),
                    "udfP95": float(row["udfP95"]),
                }
            )
    return rows


def parse_scheduler_line(line):
    marker_index = line.find(SCHEDULER_PREFIX)
    if marker_index < 0:
        return None
    payload = line[marker_index + len(SCHEDULER_PREFIX) :].strip()
    values = {}
    for token in payload.split():
        if "=" not in token:
            continue
        key, value = token.split("=", 1)
        values[key] = value
    required = {
        "wallTimeMs",
        "poolSize",
        "activeThreads",
        "queueLength",
        "cpuUsage",
        "queueEwma",
        "cpuEwma",
        "activeRatio",
        "decision",
        "reason",
        "intervalMs",
        "idleCycles",
    }
    if not required.issubset(values.keys()):
        return None
    return {
        "wallTimeMs": int(values["wallTimeMs"]),
        "poolSize": int(values["poolSize"]),
        "activeThreads": int(values["activeThreads"]),
        "queueLength": int(values["queueLength"]),
        "cpuUsage": float(values["cpuUsage"]),
        "queueEwma": float(values["queueEwma"]),
        "cpuEwma": float(values["cpuEwma"]),
        "activeRatio": float(values["activeRatio"]),
        "decision": values["decision"],
        "reason": values["reason"],
        "intervalMs": int(values["intervalMs"]),
        "idleCycles": int(values["idleCycles"]),
    }


def extract_round_from_path(path):
    match = ROUND_PATTERN.search(path.stem)
    if not match:
        return None
    return int(match.group(1))


def read_scheduler_logs(log_dir):
    per_round = defaultdict(list)
    for path in sorted(Path(log_dir).glob("*.log")):
        round_id = extract_round_from_path(path)
        if round_id is None:
            continue
        with open(path, "r", encoding="utf-8", errors="replace") as handle:
            for line in handle:
                parsed = parse_scheduler_line(line)
                if parsed is not None:
                    per_round[round_id].append(parsed)
    for round_id in per_round:
        per_round[round_id].sort(key=lambda item: item["wallTimeMs"])
    return per_round


def attach_scheduler_metrics(timeseries_rows, scheduler_logs):
    merged = []
    snapshots_by_round = {}
    timestamps_by_round = {}
    for round_id, snapshots in scheduler_logs.items():
        snapshots_by_round[round_id] = snapshots
        timestamps_by_round[round_id] = [item["wallTimeMs"] for item in snapshots]

    for row in sorted(timeseries_rows, key=lambda item: (item["round"], item["timestampSec"])):
        snapshots = snapshots_by_round.get(row["round"], [])
        timestamps = timestamps_by_round.get(row["round"], [])
        merged_row = dict(row)
        if snapshots:
            index = bisect_right(timestamps, row["sampleTimeEpochMs"]) - 1
            if index < 0:
                index = 0
            snapshot = snapshots[index]
            merged_row.update(snapshot)
        else:
            merged_row.update(
                {
                    "wallTimeMs": row["sampleTimeEpochMs"],
                    "poolSize": 0,
                    "activeThreads": 0,
                    "queueLength": 0,
                    "cpuUsage": 0.0,
                    "queueEwma": 0.0,
                    "cpuEwma": 0.0,
                    "activeRatio": 0.0,
                    "decision": "missing",
                    "reason": "missing_log",
                    "intervalMs": 0,
                    "idleCycles": 0,
                }
            )
        merged.append(merged_row)
    return merged


def aggregate_by_second(rows):
    grouped = defaultdict(list)
    for row in rows:
        grouped[(row["timestampSec"], row["phase"], row["targetConcurrency"])].append(row)

    summary_rows = []
    for key in sorted(grouped.keys()):
        timestamp_sec, phase, target_concurrency = key
        items = grouped[key]
        summary_rows.append(
            {
                "timestampSec": timestamp_sec,
                "phase": phase,
                "targetConcurrency": target_concurrency,
                "roundSamples": len(items),
                "throughputPerSecondMean": mean(
                    [item["throughputPerSecond"] for item in items]
                ),
                "udfP50Mean": mean([item["udfP50"] for item in items]),
                "udfP95Mean": mean([item["udfP95"] for item in items]),
                "poolSizeMean": mean([item["poolSize"] for item in items]),
                "activeThreadsMean": mean([item["activeThreads"] for item in items]),
                "queueLengthMean": mean([item["queueLength"] for item in items]),
                "cpuUsageMean": mean([item["cpuUsage"] for item in items]),
                "queueEwmaMean": mean([item["queueEwma"] for item in items]),
                "cpuEwmaMean": mean([item["cpuEwma"] for item in items]),
            }
        )
    return summary_rows


def write_csv(path, rows, fieldnames):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def build_phase_spans(summary_rows):
    spans = []
    grouped = defaultdict(list)
    for row in summary_rows:
        grouped[row["phase"]].append(row["timestampSec"])
    for phase in sorted(
        grouped.keys(),
        key=lambda item: min(grouped[item]),
    ):
        timestamps = sorted(grouped[phase])
        spans.append((phase, timestamps[0], timestamps[-1]))
    return spans


def add_phase_background(ax, phase_spans):
    colors = ["#f5f5f5", "#ebf3ff"]
    for idx, (phase, start, end) in enumerate(phase_spans):
        ax.axvspan(start - 0.5, end + 0.5, color=colors[idx % len(colors)], alpha=0.5)
        ax.text(
            (start + end) / 2.0,
            0.98,
            phase,
            transform=ax.get_xaxis_transform(),
            ha="center",
            va="top",
            fontsize=8,
        )


def add_plot_legend(ax):
    ax.legend(loc="upper right", bbox_to_anchor=(0.98, 0.93))


def plot(summary_rows, output_dir, title_prefix):
    try:
        import matplotlib.pyplot as plt
    except ImportError as exc:
        raise RuntimeError("matplotlib is required for plotting") from exc

    xs = [row["timestampSec"] for row in summary_rows]
    phase_spans = build_phase_spans(summary_rows)

    def save_current(fig, path):
        fig.tight_layout()
        fig.savefig(path, dpi=220)
        plt.close(fig)

    fig1, ax1 = plt.subplots(figsize=(10, 4.8))
    add_phase_background(ax1, phase_spans)
    ax1.plot(xs, [row["poolSizeMean"] for row in summary_rows], label="poolSize", linewidth=2)
    ax1.plot(
        xs,
        [row["activeThreadsMean"] for row in summary_rows],
        label="activeThreads",
        linewidth=2,
    )
    ax1.plot(
        xs,
        [row["targetConcurrency"] for row in summary_rows],
        label="targetConcurrency",
        linewidth=1.8,
        linestyle="--",
    )
    ax1.set_xlabel("Time (s)")
    ax1.set_ylabel("Threads / Concurrency")
    ax1.set_title(f"{title_prefix} - Pool Size and Active Threads")
    ax1.grid(True, linestyle="--", alpha=0.35)
    add_plot_legend(ax1)
    pool_path = output_dir / "pool_size_and_active_threads.png"
    save_current(fig1, pool_path)

    fig2, ax2 = plt.subplots(figsize=(10, 4.8))
    add_phase_background(ax2, phase_spans)
    ax2.plot(
        xs,
        [row["queueLengthMean"] for row in summary_rows],
        label="queueLength",
        linewidth=2,
    )
    ax2.plot(
        xs,
        [row["queueEwmaMean"] for row in summary_rows],
        label="queueEwma",
        linewidth=2,
    )
    ax2.set_xlabel("Time (s)")
    ax2.set_ylabel("Queue")
    ax2.set_title(f"{title_prefix} - Queue and EWMA")
    ax2.grid(True, linestyle="--", alpha=0.35)
    add_plot_legend(ax2)
    queue_path = output_dir / "queue_and_ewma.png"
    save_current(fig2, queue_path)

    fig3, ax3 = plt.subplots(figsize=(10, 4.8))
    add_phase_background(ax3, phase_spans)
    ax3.plot(xs, [row["cpuUsageMean"] for row in summary_rows], label="cpuUsage", linewidth=2)
    ax3.plot(xs, [row["cpuEwmaMean"] for row in summary_rows], label="cpuEwma", linewidth=2)
    ax3.set_xlabel("Time (s)")
    ax3.set_ylabel("CPU Usage")
    ax3.set_title(f"{title_prefix} - CPU and EWMA")
    ax3.grid(True, linestyle="--", alpha=0.35)
    add_plot_legend(ax3)
    cpu_path = output_dir / "cpu_and_ewma.png"
    save_current(fig3, cpu_path)

    fig4, ax4 = plt.subplots(figsize=(10, 4.8))
    add_phase_background(ax4, phase_spans)
    ax4.plot(
        xs,
        [row["throughputPerSecondMean"] for row in summary_rows],
        label="throughputPerSecond",
        linewidth=2,
    )
    ax4.set_xlabel("Time (s)")
    ax4.set_ylabel("Queries/s")
    ax4.set_title(f"{title_prefix} - Throughput Over Time")
    ax4.grid(True, linestyle="--", alpha=0.35)
    add_plot_legend(ax4)
    throughput_path = output_dir / "throughput_over_time.png"
    save_current(fig4, throughput_path)

    fig5, ax5 = plt.subplots(figsize=(10, 4.8))
    add_phase_background(ax5, phase_spans)
    ax5.plot(xs, [row["udfP95Mean"] for row in summary_rows], label="udfP95", linewidth=2)
    ax5.plot(xs, [row["udfP50Mean"] for row in summary_rows], label="udfP50", linewidth=1.8)
    ax5.set_xlabel("Time (s)")
    ax5.set_ylabel("Latency (ms)")
    ax5.set_title(f"{title_prefix} - UDF P95 Over Time")
    ax5.grid(True, linestyle="--", alpha=0.35)
    add_plot_legend(ax5)
    latency_path = output_dir / "udf_p95_over_time.png"
    save_current(fig5, latency_path)

    fig6, axes = plt.subplots(5, 1, figsize=(12, 14), sharex=True)
    plots = [
        (
            axes[0],
            [row["targetConcurrency"] for row in summary_rows],
            [row["poolSizeMean"] for row in summary_rows],
            [row["activeThreadsMean"] for row in summary_rows],
            "Threads / Concurrency",
            "Target, Pool, Active",
            ("targetConcurrency", "poolSize", "activeThreads"),
        ),
        (
            axes[1],
            [row["queueLengthMean"] for row in summary_rows],
            [row["queueEwmaMean"] for row in summary_rows],
            None,
            "Queue",
            "Queue and EWMA",
            ("queueLength", "queueEwma", None),
        ),
        (
            axes[2],
            [row["cpuUsageMean"] for row in summary_rows],
            [row["cpuEwmaMean"] for row in summary_rows],
            None,
            "CPU Usage",
            "CPU and EWMA",
            ("cpuUsage", "cpuEwma", None),
        ),
        (
            axes[3],
            [row["throughputPerSecondMean"] for row in summary_rows],
            None,
            None,
            "Queries/s",
            "Throughput",
            ("throughputPerSecond", None, None),
        ),
        (
            axes[4],
            [row["udfP95Mean"] for row in summary_rows],
            [row["udfP50Mean"] for row in summary_rows],
            None,
            "Latency (ms)",
            "UDF Latency",
            ("udfP95", "udfP50", None),
        ),
    ]
    for ax, series1, series2, series3, ylabel, title, labels in plots:
        add_phase_background(ax, phase_spans)
        ax.plot(xs, series1, linewidth=2, label=labels[0])
        if series2 is not None:
            ax.plot(xs, series2, linewidth=2, label=labels[1])
        if series3 is not None:
            ax.plot(xs, series3, linewidth=2, label=labels[2])
        ax.set_ylabel(ylabel)
        ax.set_title(title)
        ax.grid(True, linestyle="--", alpha=0.35)
        add_plot_legend(ax)
    axes[-1].set_xlabel("Time (s)")
    overview_path = output_dir / "adaptive_behavior_overview.png"
    save_current(fig6, overview_path)

    return [
        pool_path,
        queue_path,
        cpu_path,
        throughput_path,
        latency_path,
        overview_path,
    ]


def main():
    args = parse_args()
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    timeseries_rows = read_timeseries_rows(args.timeseries_csv)
    if not timeseries_rows:
        raise RuntimeError("no successful time series rows were found")

    scheduler_logs = read_scheduler_logs(args.log_dir)
    merged_rows = attach_scheduler_metrics(timeseries_rows, scheduler_logs)
    summary_rows = aggregate_by_second(merged_rows)

    merged_path = output_dir / "merged_timeseries_with_scheduler.csv"
    summary_path = output_dir / "summary.csv"

    write_csv(
        merged_path,
        merged_rows,
        [
            "round",
            "timestampSec",
            "sampleTimeEpochMs",
            "experimentStartEpochMs",
            "phase",
            "targetConcurrency",
            "completedRequests",
            "throughputPerSecond",
            "udfP50",
            "udfP95",
            "wallTimeMs",
            "poolSize",
            "activeThreads",
            "queueLength",
            "cpuUsage",
            "queueEwma",
            "cpuEwma",
            "activeRatio",
            "decision",
            "reason",
            "intervalMs",
            "idleCycles",
        ],
    )
    write_csv(
        summary_path,
        summary_rows,
        [
            "timestampSec",
            "phase",
            "targetConcurrency",
            "roundSamples",
            "throughputPerSecondMean",
            "udfP50Mean",
            "udfP95Mean",
            "poolSizeMean",
            "activeThreadsMean",
            "queueLengthMean",
            "cpuUsageMean",
            "queueEwmaMean",
            "cpuEwmaMean",
        ],
    )

    plot_paths = plot(summary_rows, output_dir, args.title_prefix)

    print(f"merged csv: {merged_path}")
    print(f"summary csv: {summary_path}")
    for plot_path in plot_paths:
        print(f"plot: {plot_path}")


if __name__ == "__main__":
    main()
