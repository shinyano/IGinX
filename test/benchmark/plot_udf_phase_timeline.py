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
from pathlib import Path


DEFAULT_PHASE_LABELS = [
    "Phase 1: Warmup",
    "Phase 2: Peak",
    "Phase 3: Recovery",
    "Phase 4: Rebound",
    "Phase 5: Cooldown",
]
DEFAULT_PHASE_DURATIONS = [30, 60, 40, 30, 50]
DEFAULT_PHASE_CONCURRENCIES = [4, 32, 4, 24, 2]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Plot the five-phase adaptive UDF workload timeline."
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output image path, e.g. phase_timeline.png",
    )
    parser.add_argument(
        "--title",
        default="Dynamic Workload Timeline for Adaptive Python UDF Experiment",
    )
    return parser.parse_args()


def build_step_points(durations, concurrencies):
    xs = [0]
    ys = [concurrencies[0]]
    current_time = 0
    for index, duration in enumerate(durations):
        end_time = current_time + duration
        xs.append(end_time)
        ys.append(concurrencies[index])
        if index + 1 < len(concurrencies):
            xs.append(end_time)
            ys.append(concurrencies[index + 1])
        current_time = end_time
    return xs, ys


def phase_spans(durations, labels, concurrencies):
    spans = []
    start = 0
    for label, duration, concurrency in zip(labels, durations, concurrencies):
        end = start + duration
        spans.append((label, start, end, concurrency))
        start = end
    return spans


def plot_timeline(output_path, title):
    try:
        import matplotlib.pyplot as plt
    except ImportError as exc:
        raise RuntimeError("matplotlib is required for plotting") from exc

    output_path.parent.mkdir(parents=True, exist_ok=True)

    xs, ys = build_step_points(DEFAULT_PHASE_DURATIONS, DEFAULT_PHASE_CONCURRENCIES)
    spans = phase_spans(
        DEFAULT_PHASE_DURATIONS,
        DEFAULT_PHASE_LABELS,
        DEFAULT_PHASE_CONCURRENCIES,
    )

    fig, ax = plt.subplots(figsize=(11, 4.8))

    background_colors = ["#f5f5f5", "#ebf3ff"]
    for index, (label, start, end, concurrency) in enumerate(spans):
        ax.axvspan(
            start,
            end,
            color=background_colors[index % len(background_colors)],
            alpha=0.65,
            zorder=0,
        )
        ax.text(
            (start + end) / 2.0,
            max(DEFAULT_PHASE_CONCURRENCIES) + 3.8,
            label,
            ha="center",
            va="bottom",
            fontsize=9,
        )
        ax.text(
            (start + end) / 2.0,
            concurrency + 1.2,
            f"{concurrency}",
            ha="center",
            va="bottom",
            fontsize=9,
            color="#1f4e79",
        )

    ax.step(
        xs,
        ys,
        where="post",
        linewidth=2.6,
        color="#1f77b4",
        label="Target concurrency",
        zorder=3,
    )
    ax.scatter(
        [span[1] for span in spans] + [spans[-1][2]],
        [span[3] for span in spans] + [spans[-1][3]],
        color="#1f77b4",
        s=26,
        zorder=4,
    )

    for _, start, _, _ in spans[1:]:
        ax.axvline(start, color="#9aa0a6", linestyle="--", linewidth=1.0, alpha=0.8)

    ax.set_xlim(0, sum(DEFAULT_PHASE_DURATIONS))
    ax.set_ylim(0, max(DEFAULT_PHASE_CONCURRENCIES) + 6)
    ax.set_xlabel("Time (s)")
    ax.set_ylabel("Target Concurrency")
    ax.set_title(title)
    ax.set_xticks([0, 30, 90, 130, 160, 210])
    ax.grid(True, linestyle="--", alpha=0.35)
    ax.legend(loc="upper right", bbox_to_anchor=(0.98, 0.90))

    fig.tight_layout()
    fig.savefig(output_path, dpi=220)
    plt.close(fig)


def main():
    args = parse_args()
    output_path = Path(args.output).resolve()
    plot_timeline(output_path, args.title)
    print(f"plot: {output_path}")


if __name__ == "__main__":
    main()
