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

# python "test/benchmark/analyze_best_udf_architecture_run.py" --results-dir "E:\IGinX_Lab\local\IGinX\test\benchmark\adaptive_res\ft"

import argparse
import csv
from collections import Counter, defaultdict
from pathlib import Path
from textwrap import dedent

import plot_udf_architecture_results as plotter


EXPECTED_PHASES = (
    "phase1_warmup",
    "phase2_peak",
    "phase3_recovery",
    "phase4_rebound",
    "phase5_cooldown",
)


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Pick the most thesis-friendly adaptive UDF run, plot it, "
            "and generate a Markdown explanation report."
        )
    )
    parser.add_argument("--results-dir", required=True)
    parser.add_argument("--round-dir-prefix", default="round_")
    parser.add_argument(
        "--gil-enabled",
        default="0",
        help=(
            "Filter adaptive-timeseries.csv rows by gilEnabled before selecting "
            "the effective experiment. Use an empty string to disable filtering."
        ),
    )
    parser.add_argument(
        "--analysis-dir",
        default="",
        help="Directory for ranking, filtered CSV, plots, and Markdown report.",
    )
    parser.add_argument(
        "--title-prefix",
        default="IGinX Adaptive Python UDF Dynamic Benchmark",
    )
    return parser.parse_args()


def mean(values):
    return sum(values) / len(values) if values else 0.0


def clamp(value, low=0.0, high=1.0):
    return max(low, min(high, value))


def read_raw_timeseries_rows(path):
    rows = []
    with open(path, "r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            rows.append(row)
    return rows


def dedupe_experiment_rows(rows):
    deduped = {}
    for row in rows:
        key = (
            int(row["timestampSec"]),
            row["phase"],
            int(row["targetConcurrency"]),
        )
        current = deduped.get(key)
        if current is None or int(row["sampleTimeEpochMs"]) >= int(current["sampleTimeEpochMs"]):
            deduped[key] = row
    return sorted(
        deduped.values(),
        key=lambda item: (int(item["timestampSec"]), int(item["sampleTimeEpochMs"])),
    )


def parse_timeseries_row(row):
    return {
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
        "gilEnabled": row.get("gilEnabled", ""),
        "pythonVersion": row.get("pythonVersion", ""),
    }


def select_effective_experiment(raw_rows, expected_gil_enabled):
    ok_rows = [row for row in raw_rows if row.get("status") == "ok"]
    available_gil_values = sorted({row.get("gilEnabled", "") for row in ok_rows})

    if expected_gil_enabled != "":
        ok_rows = [row for row in ok_rows if row.get("gilEnabled", "") == expected_gil_enabled]
        if not ok_rows:
            raise RuntimeError(
                f"no successful rows matched gilEnabled={expected_gil_enabled}, "
                f"available values: {available_gil_values}"
            )

    grouped = defaultdict(list)
    for row in ok_rows:
        grouped[int(row["experimentStartEpochMs"])].append(row)
    if not grouped:
        raise RuntimeError("no successful time series rows were found")

    candidates = []
    for experiment_start_ms, rows in grouped.items():
        deduped_rows = dedupe_experiment_rows(rows)
        phase_counter = Counter(row["phase"] for row in deduped_rows)
        candidates.append(
            {
                "experimentStartEpochMs": experiment_start_ms,
                "rawRows": len(rows),
                "selectedRows": len(deduped_rows),
                "uniqueTimestampCount": len({row["timestampSec"] for row in deduped_rows}),
                "phaseCounter": dict(phase_counter),
                "roundValues": sorted({row["round"] for row in deduped_rows}),
                "pythonVersions": sorted({row.get("pythonVersion", "") for row in deduped_rows}),
                "gilValues": sorted({row.get("gilEnabled", "") for row in deduped_rows}),
                "rows": deduped_rows,
            }
        )

    candidates.sort(
        key=lambda item: (
            item["uniqueTimestampCount"],
            item["selectedRows"],
            item["experimentStartEpochMs"],
        ),
        reverse=True,
    )
    return candidates[0], candidates, len(ok_rows), available_gil_values


def load_effective_timeseries(round_dir, expected_gil_enabled):
    timeseries_path = round_dir / "adaptive-timeseries.csv"
    if not timeseries_path.exists():
        raise FileNotFoundError(f"missing timeseries file: {timeseries_path}")

    raw_rows = read_raw_timeseries_rows(timeseries_path)
    selected, candidates, raw_ok_count, available_gil_values = select_effective_experiment(
        raw_rows, expected_gil_enabled
    )
    parsed_rows = [parse_timeseries_row(row) for row in selected["rows"]]

    missing_phases = [
        phase for phase in EXPECTED_PHASES if selected["phaseCounter"].get(phase, 0) == 0
    ]
    if missing_phases:
        raise RuntimeError(
            f"incomplete phase data in {round_dir}: missing {', '.join(missing_phases)}"
        )

    metadata = {
        "rawOkRows": raw_ok_count,
        "availableGilValues": ",".join(available_gil_values),
        "candidateExperimentCount": len(candidates),
        "selectedExperimentStartEpochMs": selected["experimentStartEpochMs"],
        "selectedRows": selected["selectedRows"],
        "selectedRawRows": selected["rawRows"],
        "selectedRoundValues": ",".join(selected["roundValues"]),
        "selectedPythonVersions": ",".join(selected["pythonVersions"]),
        "selectedGilValues": ",".join(selected["gilValues"]),
        "candidateExperiments": ";".join(
            f"{item['experimentStartEpochMs']}:{item['selectedRows']}"
            for item in candidates
        ),
    }
    return parsed_rows, metadata


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


def score_run(round_dir, expected_gil_enabled):
    log_dir = round_dir / "logs"
    if not log_dir.exists():
        raise FileNotFoundError(f"missing log dir: {log_dir}")

    timeseries_rows, selection_metadata = load_effective_timeseries(round_dir, expected_gil_enabled)
    scheduler_logs = plotter.read_scheduler_logs(log_dir)
    merged_rows = plotter.attach_scheduler_metrics(timeseries_rows, scheduler_logs)

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

    result = {
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
        "rawOkRows": selection_metadata["rawOkRows"],
        "candidateExperimentCount": selection_metadata["candidateExperimentCount"],
        "selectedExperimentStartEpochMs": selection_metadata["selectedExperimentStartEpochMs"],
        "selectedRows": selection_metadata["selectedRows"],
        "selectedRawRows": selection_metadata["selectedRawRows"],
        "selectedRoundValues": selection_metadata["selectedRoundValues"],
        "selectedGilValues": selection_metadata["selectedGilValues"],
        "selectedPythonVersions": selection_metadata["selectedPythonVersions"],
        "availableGilValues": selection_metadata["availableGilValues"],
        "candidateExperiments": selection_metadata["candidateExperiments"],
    }
    return result, timeseries_rows, merged_rows


def write_csv(path, rows, fieldnames):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def build_report_markdown(best, ranking, plot_paths, output_dir, filtered_timeseries_path):
    best_plot_names = "\n".join(
        f"        - `{path.name}`: `{path.relative_to(output_dir)}`" for path in plot_paths
    )

    ranking_preview = "\n".join(
        f"        | {idx} | `{item['roundDir']}` | {item['score']} | {item['avgThroughput']} | "
        f"{item['peak2P95']} | {item['selectedExperimentStartEpochMs']} |"
        for idx, item in enumerate(ranking, start=1)
    )

    report = (
        dedent(
        f"""\
        # 最佳自由线程轮次分析说明

        ## 1. 自动选优结果

        - 最佳轮次目录：`{best["roundDir"]}`
        - 综合得分：`{best["score"]}`
        - 峰值阶段平均吞吐：`{best["avgThroughput"]}` queries/s
        - `Phase 2` 最大 `udfP95`：`{best["peak2P95"]}` ms
        - `Phase 4` 最大 `udfP95`：`{best["peak4P95"]}` ms
        - 选中的 `experimentStartEpochMs`：`{best["selectedExperimentStartEpochMs"]}`
        - 选中的有效样本数：`{best["selectedRows"]}`
        - 原始 `ok` 行数：`{best["rawOkRows"]}`
        - 候选实验数：`{best["candidateExperimentCount"]}`
        - `gilEnabled` 过滤值：`{best["selectedGilValues"]}`
        - 过滤后的时序文件：`{filtered_timeseries_path.name}`

        这个脚本里的“最漂亮”不是单纯追求吞吐最高，而是优先选择最能清楚展示自适应行为的轮次。综合分数由以下分量线性加权得到：

        - `expandGainScore * 20`：`Phase 2` 相比基线的扩容幅度是否明显。
        - `phase2ResponseScore * 15`：`Phase 2` 中排队出现后，线程池是否及时扩容。
        - `recoveryScore * 10`：`Phase 3` 回落后是否回收到更接近基线的规模。
        - `shrinkScore * 15`：`Phase 5` 深度冷却阶段是否继续缩容。
        - `reboundGainScore * 15`：`Phase 4` 是否再次明显扩容。
        - `phase4ResponseScore * 10`：`Phase 4` 的二次高峰响应是否及时。
        - `queueVisibilityScore * 5`：高峰期是否出现足够清晰的排队信号。
        - `stabilityScore * 5`：整体线程池变化是否避免高频振荡。
        - `latencyScore * 5`：高峰期延迟是否没有坏到掩盖自适应行为。

        ## 2. 数据清洗与对齐方法

        该脚本对每个 `round_*` 目录按下面流程处理，因此即使某些 CSV 有历史残留，也不会直接把脏数据拿来评分或出图：

        1. 读取 `adaptive-timeseries.csv`，仅保留 `status == ok` 的行。
        2. 若指定了 `--gil-enabled`，进一步只保留匹配该值的行；当前自由线程分析默认使用 `gilEnabled=0`。
        3. 按 `experimentStartEpochMs` 分组，把同一实验内重复拼接的秒级样本按 `(timestampSec, phase, targetConcurrency)` 去重。
        4. 优先选择“唯一秒数最多”的实验；如果完整度相同，则选 `experimentStartEpochMs` 更新的一组。
        5. 读取对应目录下 `logs/*.log` 中的 `UDF_ADAPTIVE_METRICS` 快照。
        6. 对每个秒级客户端样本，选择满足 `wallTimeMs <= sampleTimeEpochMs` 的最近一条调度快照进行对齐。
        7. 对相同 `(timestampSec, phase, targetConcurrency)` 的记录计算均值，写入 `summary.csv`，再用于画图。

        因而：

        - `summary.csv` 里的 `*Mean` 字段表示“按秒聚合后的均值”。
        - 对于当前“单个最佳轮次”的出图结果，`roundSamples=1`，所以这些 `*Mean` 数值实际上就等于该轮次对应秒的原始值。

        ## 3. 排名概览

        | 排名 | 轮次 | 综合得分 | 峰值阶段平均吞吐 | `Phase 2` 最大 `udfP95` | 选中实验 |
        | --- | --- | ---: | ---: | ---: | ---: |
        {ranking_preview}

        ## 4. 产出文件

        - `round_ranking.csv`：六轮评分与关键指标。
        - `best_run.txt`：最佳轮次的简要摘要。
        - `{filtered_timeseries_path.name}`：最佳轮次过滤并去重后的秒级样本。
        - `merged_timeseries_with_scheduler.csv`：客户端样本与调度快照按时间对齐后的逐秒明细。
        - `summary.csv`：逐秒聚合后的绘图输入。
        {best_plot_names}

        ## 5. 每张图的详细解释

        ### 5.1 `pool_size_and_active_threads.png`

        - 横坐标：`timestampSec`，表示从实验开始起算的第几秒。
        - 纵坐标：`Threads / Concurrency`。
        - 曲线含义：
          - `targetConcurrency`：该秒目标并发数，由负载阶段配置直接给出。
          - `poolSizeMean`：对齐后的调度快照里线程池总大小。
          - `activeThreadsMean`：该秒真正处于活跃状态的线程数。
        - 计算方法：
          - `poolSize` 和 `activeThreads` 取自 `UDF_ADAPTIVE_METRICS`。
          - 每秒取最近一条不晚于该秒采样时刻的快照，再做逐秒均值聚合。
        - 最能验证的预期：
          - `Phase 2` 发生明显扩容。
          - `Phase 3` 和 `Phase 5` 逐步缩容。
          - `Phase 4` 再次扩容，且响应不慢于首次高峰。
          - 是否存在高频振荡。
        - 可验证预期程度：`高`。这是判断自适应线程池“有没有按阶段扩缩容”的主图。

        ### 5.2 `queue_and_ewma.png`

        - 横坐标：`timestampSec`。
        - 纵坐标：`Queue`。
        - 曲线含义：
          - `queueLengthMean`：调度快照中的当前队列长度。
          - `queueEwmaMean`：队列长度的 EWMA 平滑值。
        - 计算方法：
          - 两条曲线都来自 `UDF_ADAPTIVE_METRICS` 原始字段，脚本不重新计算 EWMA，只做时序对齐与均值聚合。
        - 最能验证的预期：
          - 高峰来临时是否先出现排队。
          - 扩容是否跟随排队信号触发，而不是无缘无故扩容。
          - 冷却阶段排队是否消退。
        - 可验证预期程度：`高`。它最适合验证“先排队、后扩容”的因果顺序，但不能单独说明扩容幅度是否合适。

        ### 5.3 `cpu_and_ewma.png`

        - 横坐标：`timestampSec`。
        - 纵坐标：`CPU Usage`，通常是 `0.0 ~ 1.0` 的比例值。
        - 曲线含义：
          - `cpuUsageMean`：调度采样时刻的瞬时 CPU 使用率。
          - `cpuEwmaMean`：CPU 使用率的 EWMA 平滑值。
        - 计算方法：
          - 二者同样直接来自 `UDF_ADAPTIVE_METRICS`，只经过按秒对齐与均值聚合。
        - 最能验证的预期：
          - 高峰期 CPU 是否真实升高。
          - 缩容滞后时是否存在 CPU 仍偏高的解释。
          - 扩容慢时是否可能是 CPU 抑制阈值过于保守。
        - 可验证预期程度：`中`。它更偏“解释图”，适合辅助说明为什么扩容/缩容会提前或滞后。

        ### 5.4 `throughput_over_time.png`

        - 横坐标：`timestampSec`。
        - 纵坐标：`Queries/s`。
        - 曲线含义：
          - `throughputPerSecondMean`：每秒完成请求数。
        - 计算方法：
          - 该字段来自 benchmark runner。
          - 当前实验实现中，`throughputPerSecond` 直接等于该秒 `completedRequests`，也就是该秒 drain 出来的完成请求条数，而不是额外平滑后的吞吐估计值。
        - 最能验证的预期：
          - 高峰期扩容后是否带来更高吞吐。
          - 回落和冷却阶段吞吐是否随目标并发下降。
        - 可验证预期程度：`中`。它能体现用户侧效果，但无法单独区分“调度策略问题”和“Python 执行瓶颈”。

        ### 5.5 `udf_p95_over_time.png`

        - 横坐标：`timestampSec`。
        - 纵坐标：`Latency (ms)`。
        - 曲线含义：
          - `udfP95Mean`：该秒完成请求时延的 95 分位。
          - `udfP50Mean`：该秒完成请求时延的 50 分位。
        - 计算方法：
          - 二者由 benchmark runner 基于该秒已完成请求的时延样本计算。
          - 绘图脚本只做逐秒均值聚合，不重新计算分位数。
        - 最能验证的预期：
          - 高峰期排队是否导致尾延迟上升。
          - 扩容后尾延迟是否回落或趋稳。
          - 二次高峰是否优于首次冷启动。
        - 可验证预期程度：`中高`。它对“自适应是否改善用户感知延迟”很重要，但属于结果图，不是最直接的调度内部证据。

        ### 5.6 `adaptive_behavior_overview.png`

        - 横坐标：`timestampSec`。
        - 纵坐标：分 5 个子图分别对应线程规模、队列、CPU、吞吐、延迟。
        - 图的意义：
          - 它把上述五类指标放在同一页里，便于顺着五个实验阶段整体阅读。
          - 论文或汇报里通常最适合作为“一张总览图”，再配合前面几张单图展开解释。
        - 可验证预期程度：`高`。它最适合做总体叙事，但如果要做精确论证，仍建议回看对应的单图和 `summary.csv`。

        ## 6. 如何用这些图去验证实验设计中的预期

        结合 `UDF_ARCHITECTURE_EXPERIMENT_DESIGN.md` 第 11.1 节，可以按下面方式读图：

        1. 先看 `queue_and_ewma.png`，确认 `Phase 2` 与 `Phase 4` 是否先出现排队压力。
        2. 再看 `pool_size_and_active_threads.png`，确认排队后是否出现明显扩容，以及 `Phase 3/5` 是否逐步缩容。
        3. 用 `cpu_and_ewma.png` 判断扩缩容节奏是否可能受 CPU 抑制阈值影响。
        4. 用 `throughput_over_time.png` 与 `udf_p95_over_time.png` 观察这些调度动作有没有转化成外部吞吐改善和尾延迟缓解。
        5. 最后回到 `adaptive_behavior_overview.png`，检查整个五阶段过程是否叙事完整、是否存在明显振荡或解释断裂。

        ## 7. 结果解读边界

        - 这些图可以很好地验证“调度现象是否出现”，但不能单独证明所有性能收益都来自线程池策略本身。
        - `throughput_over_time.png` 和 `udf_p95_over_time.png` 仍会受到 Python 运行时、UDF 实现、系统资源竞争等因素影响。
        - 如果后续要写论文正文，建议同时引用 `round_ranking.csv` 的评分结果和 `summary.csv` 的具体数值，这样论证会更扎实。
        """
    )
        .strip()
        + "\n"
    )
    report = report.replace("        | 1 |", "| 1 |")
    report = report.replace(
        "        - `pool_size_and_active_threads.png`",
        "- `pool_size_and_active_threads.png`",
    )
    return report


def main():
    args = parse_args()
    results_dir = Path(args.results_dir).resolve()
    analysis_dir = (
        Path(args.analysis_dir).resolve()
        if args.analysis_dir
        else (results_dir / "best_run_analysis").resolve()
    )
    analysis_dir.mkdir(parents=True, exist_ok=True)

    round_dirs = sorted(
        path
        for path in results_dir.iterdir()
        if path.is_dir() and path.name.startswith(args.round_dir_prefix)
    )
    if not round_dirs:
        raise RuntimeError(f"no round directories found under {results_dir}")

    scoring_results = [score_run(round_dir, args.gil_enabled) for round_dir in round_dirs]
    ranking = [item[0] for item in scoring_results]
    by_round_name = {item[0]["roundDir"]: item for item in scoring_results}
    ranking.sort(key=lambda item: (item["score"], item["avgThroughput"]), reverse=True)

    best = ranking[0]
    best_timeseries_rows = by_round_name[best["roundDir"]][1]
    best_merged_rows = by_round_name[best["roundDir"]][2]
    best_round_dir = results_dir / best["roundDir"]

    filtered_timeseries_path = analysis_dir / "best_timeseries_filtered.csv"
    write_csv(
        filtered_timeseries_path,
        best_timeseries_rows,
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
            "gilEnabled",
            "pythonVersion",
        ],
    )

    merged_path = analysis_dir / "merged_timeseries_with_scheduler.csv"
    summary_path = analysis_dir / "summary.csv"
    summary_rows = plotter.aggregate_by_second(best_merged_rows)
    plotter.write_csv(
        merged_path,
        best_merged_rows,
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
             "gilEnabled",
             "pythonVersion",
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
    plotter.write_csv(
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

    title_prefix = args.title_prefix
    plot_paths = plotter.plot(summary_rows, analysis_dir, title_prefix)

    ranking_path = analysis_dir / "round_ranking.csv"
    write_csv(ranking_path, ranking, list(ranking[0].keys()))

    best_run_txt_path = analysis_dir / "best_run.txt"
    best_run_txt_path.write_text(
        "\n".join(
            [
                f"best_round={best['roundDir']}",
                f"score={best['score']}",
                f"selected_experiment_start_epoch_ms={best['selectedExperimentStartEpochMs']}",
                f"selected_rows={best['selectedRows']}",
                f"avg_throughput={best['avgThroughput']}",
                (
                    "reason="
                    f"phase2_expand({best['expandGainScore']}), "
                    f"phase3_recovery({best['recoveryScore']}), "
                    f"phase5_shrink({best['shrinkScore']}), "
                    f"phase4_rebound({best['reboundGainScore']}), "
                    f"stability({best['stabilityScore']})"
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    report_path = analysis_dir / "best_run_report.md"
    report_path.write_text(
        build_report_markdown(best, ranking, plot_paths, analysis_dir, filtered_timeseries_path),
        encoding="utf-8",
    )

    print(f"best round: {best['roundDir']}")
    print(f"analysis dir: {analysis_dir}")
    print(f"ranking csv: {ranking_path}")
    print(f"best summary: {best_run_txt_path}")
    print(f"report: {report_path}")
    print(f"filtered timeseries: {filtered_timeseries_path}")
    print(f"merged csv: {merged_path}")
    print(f"summary csv: {summary_path}")
    for plot_path in plot_paths:
        print(f"plot: {plot_path}")
    print(f"best source dir: {best_round_dir}")


if __name__ == "__main__":
    main()
