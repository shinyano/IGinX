# Python UDF 自适应线程池动态实验说明

这套实验工具不再做 `legacy vs adaptive` 的历史架构对比，而是专注于当前自适应 Python UDF 线程池在动态负载下的扩缩容行为观测。

实验主线与 `test/benchmark/UDF_ARCHITECTURE_EXPERIMENT_DESIGN.md` 保持一致：

- 采用五阶段阶梯式负载：预热 -> 首次高峰 -> 回落 -> 二次高峰 -> 深度冷却
- 客户端 runner 负责驱动动态负载并输出秒级时间序列
- 服务端 `AdaptiveScheduler` 负责输出结构化调度日志
- 本机绘图脚本按时间对齐客户端时序和服务端调度指标

## 文件对应关系

- Java runner：
  `core/src/main/java/cn/edu/tsinghua/iginx/tools/benchmark/AdaptiveUdfArchitectureBenchmarkRunner.java`
- 服务器脚本：
  `core/src/assembly/resources/benchmark/run_udf_architecture_matrix.sh`
- UDF 脚本：
  `core/src/assembly/resources/benchmark/udf/architecture_benchmark_metadata_udsf.py`
  `core/src/assembly/resources/benchmark/udf/architecture_benchmark_compute_udsf.py`
- 本机绘图脚本：
  `test/benchmark/plot_udf_architecture_results.py`
- 实验设计文档：
  `test/benchmark/UDF_ARCHITECTURE_EXPERIMENT_DESIGN.md`

## 默认实验口径

默认采用自由线程 Python 运行时，并使用 `DataFrame -> NumPy` 向量化 workload：

- 输入数据规模：`rows=10000`, `cols=10`
- 每次 UDF 内部计算轮数：`loops=20`
- 默认时间线：`30,60,40,30,50` 秒
- 默认并发曲线：`4,32,4,24,2`

这套口径默认以 free-threading 为运行时基础，用于观察当前调度池在真实 `pandas/numpy` 风格 UDF 下的资源弹性行为。

## 前置条件

1. 在本机完成 `core` 模块编译与打包。
2. 将发行目录上传到远程服务器。
3. 服务器端可正常启动 IGinX 及其依赖服务。
4. 自由线程 Python 环境包含 `numpy`、`pandas` 和 `iginx_udf`。

## 最小 Smoke Test

先执行一轮缩短版动态实验，确认链路可用：

```bash
cd /path/to/iginx-release

export CONDA_BASE=/root/anaconda3
export PYTHON_CONDA_ENV=py313_ft
export ROUNDS=1

export ROWS=10000
export COLS=10
export LOOPS=20
export WARMUP=1
export WARMUP_INVOCATIONS_PER_THREAD=1
export WARMUP_SETTLE_MS=1000

export PHASE_DURATIONS="10,20,15,15,20"
export PHASE_CONCURRENCIES="4,16,4,12,2"

bash benchmark/run_udf_architecture_matrix.sh
```

如果 smoke test 成功，说明：

- runner 能注册 UDF、写入数据并执行动态五阶段负载
- `adaptive-timeseries.csv` 能正常生成
- 每轮服务端日志包含 `UDF_ADAPTIVE_METRICS` 结构化调度快照
- 本机绘图脚本可以完成时序对齐与出图

## 正式实验

推荐正式实验按论文主线配置运行。若你想一次性执行五轮正式实验，只需把 `ROUNDS` 设为 `5`：

```bash
cd /path/to/iginx-release

export CONDA_BASE=/root/anaconda3
export PYTHON_CONDA_ENV=py313_ft
export PORTS_TO_CLEAN="6888 7888"

export ROUNDS=5

export ROWS=10000
export COLS=10
export LOOPS=20
export WARMUP=2
export WARMUP_INVOCATIONS_PER_THREAD=2
export WARMUP_SETTLE_MS=2500

export PHASE_DURATIONS="30,60,40,30,50"
export PHASE_CONCURRENCIES="4,32,4,24,2"

export ADAPTIVE_INITIAL_THREADS=4
export ADAPTIVE_MIN_THREADS=4
export ADAPTIVE_MAX_THREADS=96
export UDF_POOL_EXPAND_THRESHOLD=5
export UDF_POOL_SHRINK_THRESHOLD=1
export UDF_POOL_CPU_HIGH_THRESHOLD=0.85
export UDF_POOL_CPU_LOW_THRESHOLD=0.30
export UDF_POOL_SCHEDULE_INTERVAL_MS=2000
export UDF_POOL_COOLDOWN_MS=5000

bash benchmark/run_udf_architecture_matrix.sh | tee benchmark/results/architecture/dynamic-run.log
```

如果在服务器上运行，建议放进 `tmux`：

```bash
tmux new -s udf_adaptive_dynamic
```

在 `tmux` 中执行正式实验命令，完成后可用以下命令查看状态：

```bash
tmux ls
tmux attach -t udf_adaptive_dynamic
tail -f benchmark/results/architecture/dynamic-run.log
ls -ltr benchmark/results/architecture/logs
```

## 输出结果

默认输出为“每轮一个目录”：

- `benchmark/results/architecture/round_1/adaptive-timeseries.csv`
- `benchmark/results/architecture/round_1/logs/adaptive-dynamic-r1.log`
- `benchmark/results/architecture/round_2/...`
- `benchmark/results/architecture/round_5/...`
- `benchmark/results/architecture/config.properties.bak`

其中：

- 每个 `round_*` 目录对应一次完整五阶段场景模拟
- `adaptive-timeseries.csv` 是该轮客户端秒级观测结果
- `logs/adaptive-dynamic-r*.log` 同时包含该轮 IGinX 服务日志和 `UDF_ADAPTIVE_METRICS` 结构化调度快照

## 时间序列字段

客户端时序 CSV 主要字段：

- `round`
- `timestampSec`
- `sampleTimeEpochMs`
- `experimentStartEpochMs`
- `phase`
- `targetConcurrency`
- `completedRequests`
- `throughputPerSecond`
- `udfP50`
- `udfP95`

服务端调度日志主要字段：

- `wallTimeMs`
- `poolSize`
- `activeThreads`
- `queueLength`
- `cpuUsage`
- `queueEwma`
- `cpuEwma`
- `decision`
- `reason`

## Warmup 策略

当前 warmup 不是轻量单次调用，而是面向正式实验的并发预热：

- 预热并发至少覆盖初始线程数和第一阶段并发
- 每个 warmup worker 连续执行 `WARMUP_INVOCATIONS_PER_THREAD` 次 UDF
- 每轮预热后等待 `WARMUP_SETTLE_MS`

这样可以减少正式测量阶段被线程池冷启动和线程本地解释器首次创建污染。

## 本机汇总与画图

如果你要对单轮结果画图，拉回本机后执行：

```bash
python test/benchmark/plot_udf_architecture_results.py \
  --timeseries-csv /path/to/round_1/adaptive-timeseries.csv \
  --log-dir /path/to/round_1/logs \
  --output-dir /path/to/plots
```

输出包括：

- `merged_timeseries_with_scheduler.csv`
- `summary.csv`
- `pool_size_and_active_threads.png`
- `queue_and_ewma.png`
- `cpu_and_ewma.png`
- `throughput_over_time.png`
- `udf_p95_over_time.png`
- `adaptive_behavior_overview.png`

## 自动挑选最漂亮的一轮

如果你一次跑了多轮正式实验，可以让脚本自动挑出“最符合预期扩缩容行为”的那一轮：

```bash
python test/benchmark/select_best_udf_architecture_run.py \
  --results-dir /path/to/benchmark/results/architecture
```

默认输出：

- `round_ranking.csv`
- `best_run.txt`

筛选逻辑会综合考虑：

- `phase2_peak` 是否发生明显扩容
- `phase3_recovery` 是否开始回落
- `phase5_cooldown` 是否缩回接近基线
- `phase4_rebound` 是否再次拉起
- 扩缩容过程中是否过于振荡
- 高峰阶段 `udfP95` 是否过高

## 结果解释建议

- 如果 `phase2_peak` 中 `queueLength / queueEwma` 先升高，随后 `poolSize` 上升，说明扩容链条与设计预期一致。
- 如果 `phase3_recovery` 和 `phase5_cooldown` 中 `queueLength` 下降后 `poolSize` 逐步回落，说明缩容逻辑在冷却窗口后生效。
- 如果 `phase4_rebound` 中再次扩容更快，且没有明显过冲，说明二次高峰响应较稳定。
- 如果高峰阶段扩容不明显，应优先检查队列阈值、CPU 抑制阈值、调度周期和冷却时间是否过于保守。

