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
from pathlib import Path


SCHEDULER_PREFIX = "UDF_ADAPTIVE_METRICS "


def parse_args():
    parser = argparse.ArgumentParser(
        description="Rank adaptive UDF experiment rounds and pick the most thesis-friendly run."
    )
    parser.add_argument("--results-dir", required=True)
    parser.add_argument("--round-dir-prefix", default="round_")
    parser.add_argument("--output-csv", default="")
    parser.add_argument("--output-text", default="")
    return parser.parse_args()


def mean(values):
    return sum(values) / len(values) if values else 0.0


def clamp(value, low=0.0, high=1.0):
    return max(low, min(high, value))


def read_timeseries_rows(path):
    rows = []
    with open(path, "r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if row.get("status") != "ok":
                continue
            rows.append(
                {
                    "timestampSec": int(row["timestampSec"]),
                    "phase": row["phase"],
                    "targetConcurrency": int(row["targetConcurrency"]),
                    "throughputPerSecond": float(row["throughputPerSecond"]),
                    "udfP50": float(row["udfP50"]),
                    "udfP95": float(row["udfP95"]),
                    "sampleTimeEpochMs": int(row["sampleTimeEpochMs"]),
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
        "decision",
        "reason",
    }
    if not required.issubset(values.keys()):
        return None
    return {
        "wallTimeMs": int(values["wallTimeMs"]),
        "poolSize": float(values["poolSize"]),
        "activeThreads": float(values["activeThreads"]),
        "queueLength": float(values["queueLength"]),
        "cpuUsage": float(values["cpuUsage"]),
        "queueEwma": float(values["queueEwma"]),
        "cpuEwma": float(values["cpuEwma"]),
        "decision": values["decision"],
        "reason": values["reason"],
    }


def read_scheduler_rows(log_dir):
    rows = []
    for log_path in sorted(Path(log_dir).glob("*.log")):
        with open(log_path, "r", encoding="utf-8", errors="replace") as handle:
            for line in handle:
                parsed = parse_scheduler_line(line)
                if parsed is not None:
                    rows.append(parsed)
    rows.sort(key=lambda item: item["wallTimeMs"])
    return rows


def attach_scheduler_rows(timeseries_rows, scheduler_rows):
    if not scheduler_rows:
        merged = []
        for row in timeseries_rows:
            merged_row = dict(row)
            merged_row.update(
                {
                    "poolSize": 0.0,
                    "activeThreads": 0.0,
                    "queueLength": 0.0,
                    "cpuUsage": 0.0,
                    "queueEwma": 0.0,
                    "cpuEwma": 0.0,
                    "decision": "missing",
                    "reason": "missing_log",
                }
            )
            merged.append(merged_row)
        return merged

    timestamps = [item["wallTimeMs"] for item in scheduler_rows]
    merged = []
    for row in timeseries_rows:
        index = bisect_right(timestamps, row["sampleTimeEpochMs"]) - 1
        if index < 0:
            index = 0
        merged_row = dict(row)
        merged_row.update(scheduler_rows[index])
        merged.append(merged_row)
    return merged


def rows_for_phase(rows, phase_name):
    return [row for row in rows if row["phase"] == phase_name]


def first_pool_increase_time(rows, baseline_pool):
    for row in rows:
        if row["poolSize"] > baseline_pool + 0.5:
            return row["timestampSec"]
    return None


def first_queue_rise_time(rows):
    for row in rows:
        if row["queueLength"] > 0.5 or row["queueEwma"] > 0.5:
            return row["timestampSec"]
    return None


def oscillation_penalty(pool_sizes):
    if len(pool_sizes) < 3:
        return 0.0
    direction_changes = 0
    last_direction = 0
    for idx in range(1, len(pool_sizes)):
        diff = pool_sizes[idx] - pool_sizes[idx - 1]
        direction = 1 if diff > 0.1 else -1 if diff < -0.1 else 0
        if direction != 0 and last_direction != 0 and direction != last_direction:
            direction_changes += 1
        if direction != 0:
            last_direction = direction
    return float(direction_changes)


def score_run(round_dir):
    timeseries_path = round_dir / "adaptive-timeseries.csv"
    log_dir = round_dir / "logs"
    if not timeseries_path.exists():
        raise FileNotFoundError(f"missing timeseries file: {timeseries_path}")
    if not log_dir.exists():
        raise FileNotFoundError(f"missing log dir: {log_dir}")

    timeseries_rows = read_timeseries_rows(timeseries_path)
    scheduler_rows = read_scheduler_rows(log_dir)
    merged_rows = attach_scheduler_rows(timeseries_rows, scheduler_rows)

    phase1 = rows_for_phase(merged_rows, "phase1_warmup")
    phase2 = rows_for_phase(merged_rows, "phase2_peak")
    phase3 = rows_for_phase(merged_rows, "phase3_recovery")
    phase4 = rows_for_phase(merged_rows, "phase4_rebound")
    phase5 = rows_for_phase(merged_rows, "phase5_cooldown")

    if not all([phase1, phase2, phase3, phase4, phase5]):
      raise RuntimeError(f"incomplete phase data in {round_dir}")

    baseline_pool = mean([row["poolSize"] for row in phase1])
    peak2_pool = max(row["poolSize"] for row in phase2)
    recovery_pool = mean([row["poolSize"] for row in phase3[-max(1, len(phase3) // 4) :]])
    peak4_pool = max(row["poolSize"] for row in phase4)
    cooldown_pool = mean([row["poolSize"] for row in phase5[-max(1, len(phase5) // 4) :]])

    peak2_queue = max(row["queueLength"] for row in phase2)
    peak4_queue = max(row["queueLength"] for row in phase4)
    peak2_p95 = max(row["udfP95"] for row in phase2)
    peak4_p95 = max(row["udfP95"] for row in phase4)
    avg_throughput = mean([row["throughputPerSecond"] for row in phase2 + phase4])

    phase2_queue_rise = first_queue_rise_time(phase2)
    phase2_expand = first_pool_increase_time(phase2, baseline_pool)
    phase4_queue_rise = first_queue_rise_time(phase4)
    phase4_expand = first_pool_increase_time(phase4, recovery_pool)

    phase2_response_score = 0.5
    if phase2_queue_rise is not None and phase2_expand is not None:
        phase2_response_score = clamp(1.0 - (phase2_expand - phase2_queue_rise) / 20.0)
    elif phase2_expand is not None:
        phase2_response_score = 0.7
    elif peak2_pool > baseline_pool + 0.5:
        phase2_response_score = 0.6
    else:
        phase2_response_score = 0.0

    phase4_response_score = 0.5
    if phase4_queue_rise is not None and phase4_expand is not None:
        phase4_response_score = clamp(1.0 - (phase4_expand - phase4_queue_rise) / 15.0)
    elif phase4_expand is not None:
        phase4_response_score = 0.7
    elif peak4_pool > recovery_pool + 0.5:
        phase4_response_score = 0.6
    else:
        phase4_response_score = 0.0

    expand_gain_score = clamp((peak2_pool - baseline_pool) / max(1.0, baseline_pool))
    rebound_gain_score = clamp((peak4_pool - recovery_pool) / max(1.0, peak2_pool - baseline_pool))
    shrink_score = clamp((peak2_pool - cooldown_pool) / max(1.0, peak2_pool - baseline_pool))
    recovery_score = clamp((peak2_pool - recovery_pool) / max(1.0, peak2_pool - baseline_pool))

    queue_visibility_score = clamp(max(peak2_queue, peak4_queue) / 4.0)

    oscillation = oscillation_penalty([row["poolSize"] for row in merged_rows])
    stability_score = clamp(1.0 - oscillation / 8.0)

    latency_baseline = max(1.0, mean([row["udfP95"] for row in phase1]))
    latency_penalty = (peak2_p95 + peak4_p95) / (2.0 * latency_baseline)
    latency_score = clamp(1.5 - latency_penalty, 0.0, 1.0)

    score = (
        expand_gain_score * 20.0
        + phase2_response_score * 15.0
        + recovery_score * 10.0
        + shrink_score * 15.0
        + rebound_gain_score * 15.0
        + phase4_response_score * 10.0
        + queue_visibility_score * 5.0
        + stability_score * 5.0
        + latency_score * 5.0
    )

    return {
        "roundDir": round_dir.name,
        "score": round(score, 4),
        "baselinePool": round(baseline_pool, 4),
        "peak2Pool": round(peak2_pool, 4),
        "recoveryPool": round(recovery_pool, 4),
        "peak4Pool": round(peak4_pool, 4),
        "cooldownPool": round(cooldown_pool, 4),
        "peak2Queue": round(peak2_queue, 4),
        "peak4Queue": round(peak4_queue, 4),
        "peak2P95": round(peak2_p95, 4),
        "peak4P95": round(peak4_p95, 4),
        "avgThroughput": round(avg_throughput, 4),
        "expandGainScore": round(expand_gain_score, 4),
        "phase2ResponseScore": round(phase2_response_score, 4),
        "recoveryScore": round(recovery_score, 4),
        "shrinkScore": round(shrink_score, 4),
        "reboundGainScore": round(rebound_gain_score, 4),
        "phase4ResponseScore": round(phase4_response_score, 4),
        "queueVisibilityScore": round(queue_visibility_score, 4),
        "stabilityScore": round(stability_score, 4),
        "latencyScore": round(latency_score, 4),
        "phase2FirstQueueRise": "" if phase2_queue_rise is None else phase2_queue_rise,
        "phase2FirstExpand": "" if phase2_expand is None else phase2_expand,
        "phase4FirstQueueRise": "" if phase4_queue_rise is None else phase4_queue_rise,
        "phase4FirstExpand": "" if phase4_expand is None else phase4_expand,
    }


def main():
    args = parse_args()
    results_dir = Path(args.results_dir).resolve()
    round_dirs = sorted(
        path
        for path in results_dir.iterdir()
        if path.is_dir() and path.name.startswith(args.round_dir_prefix)
    )
    if not round_dirs:
        raise RuntimeError(f"no round directories found under {results_dir}")

    ranking = [score_run(round_dir) for round_dir in round_dirs]
    ranking.sort(key=lambda item: item["score"], reverse=True)

    output_csv = (
        Path(args.output_csv).resolve()
        if args.output_csv
        else results_dir / "round_ranking.csv"
    )
    output_text = (
        Path(args.output_text).resolve()
        if args.output_text
        else results_dir / "best_run.txt"
    )

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with open(output_csv, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(ranking[0].keys()))
        writer.writeheader()
        for row in ranking:
            writer.writerow(row)

    best = ranking[0]
    lines = [
        f"best_round={best['roundDir']}",
        f"score={best['score']}",
        f"reason=phase2_expand({best['expandGainScore']}), phase3_recovery({best['recoveryScore']}), phase5_shrink({best['shrinkScore']}), phase4_rebound({best['reboundGainScore']}), stability({best['stabilityScore']})",
    ]
    output_text.parent.mkdir(parents=True, exist_ok=True)
    output_text.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"best round: {best['roundDir']}")
    print(f"ranking csv: {output_csv}")
    print(f"summary txt: {output_text}")


if __name__ == "__main__":
    main()
