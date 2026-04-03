# 最佳自由线程轮次分析说明

## 1. 自动选优结果

- 最佳轮次目录：`round_2`
- 综合得分：`87.6845`
- 峰值阶段平均吞吐：`33.4556` queries/s
- `Phase 2` 最大 `udfP95`：`1521.2214` ms
- `Phase 4` 最大 `udfP95`：`1292.8007` ms
- 选中的 `experimentStartEpochMs`：`1775146723831`
- 选中的有效样本数：`210`
- 原始 `ok` 行数：`210`
- 候选实验数：`1`
- `gilEnabled` 过滤值：`0`
- 过滤后的时序文件：`best_timeseries_filtered.csv`

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

| 排名 |    轮次     |    综合得分 | 峰值阶段平均吞吐 | `Phase 2` 最大 `udfP95` |          选中实验 |
|----|-----------|--------:|---------:|----------------------:|--------------:|
| 1  | `round_2` | 87.6845 |  33.4556 |             1521.2214 | 1775146723831 |
| 2  | `round_4` |  87.506 |  21.2889 |             1892.7936 | 1775147178949 |
| 3  | `round_5` | 86.3512 |  26.4778 |             2100.9269 | 1775147406559 |
| 4  | `round_3` | 86.3512 |  24.4111 |             1842.3934 | 1775146951658 |
| 5  | `round_1` | 86.1726 |     21.8 |             2167.0174 | 1775146496264 |
| 6  | `round_0` | 85.8155 |  41.3778 |             1784.8415 | 1775145367839 |

## 4. 产出文件

- `round_ranking.csv`：六轮评分与关键指标。
- `best_run.txt`：最佳轮次的简要摘要。
- `best_timeseries_filtered.csv`：最佳轮次过滤并去重后的秒级样本。
- `merged_timeseries_with_scheduler.csv`：客户端样本与调度快照按时间对齐后的逐秒明细。
- `summary.csv`：逐秒聚合后的绘图输入。
- `pool_size_and_active_threads.png`: `pool_size_and_active_threads.png`
- `queue_and_ewma.png`: `queue_and_ewma.png`
- `cpu_and_ewma.png`: `cpu_and_ewma.png`
- `throughput_over_time.png`: `throughput_over_time.png`
- `udf_p95_over_time.png`: `udf_p95_over_time.png`
- `adaptive_behavior_overview.png`: `adaptive_behavior_overview.png`

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

