# Python UDF 架构对比实验使用说明

这套实验工具用于评估：

- `legacy`：关闭自适应 UDF 池
- `adaptive`：开启独立自适应 UDF 池

整个流程保持“本机编译、服务器只运行”。

## 文件对应关系

本实验新增了一套独立文件，不会影响已有自由线程实验：

- Java runner：
  `core/src/main/java/cn/edu/tsinghua/iginx/tools/benchmark/AdaptiveUdfArchitectureBenchmarkRunner.java`
- 服务器脚本：
  `core/src/assembly/resources/benchmark/run_udf_architecture_matrix.sh`
- UDF 脚本：
  `core/src/assembly/resources/benchmark/udf/latency_benchmark_udsf.py`
- 本机绘图脚本：
  `test/benchmark/plot_udf_architecture_results.py`
- 实验设计文档：
  `test/benchmark/UDF_ARCHITECTURE_EXPERIMENT_DESIGN.md`

## 服务器端运行目录

打包后的发行包中需要包含：

- `lib/*`
- `conf/config.properties`
- `sbin/start_iginx.sh`
- `benchmark/run_udf_architecture_matrix.sh`
- `benchmark/udf/latency_benchmark_udsf.py`

## 前置条件

1. 在本机完成编译和打包。
2. 把运行目录上传到服务器。
3. 服务器能正常启动 IGinX 及其依赖服务。
4. 服务器上准备好 Python 运行环境。

## 最小 Smoke Test

先只跑一轮最小验证：

```bash
cd /path/to/iginx-release

export CONDA_BASE=/path/to/miniconda3
export PYTHON_CONDA_ENV=py313_std
export ARCHITECTURES="legacy adaptive"
export CONCURRENCIES="4"
export ROUNDS=1

export ROWS=10000
export COLS=10
export LOOPS=20
export STEADY_INVOCATIONS_PER_THREAD=3
export WARMUP=1

export MIXED_DURATION_SECONDS=10
export LIGHT_QUERY_CONCURRENCY=1
export LIGHT_QUERY_LIMIT=128

bash benchmark/run_udf_architecture_matrix.sh
```

如果 smoke test 成功，说明：

- 两种架构切换正常
- runner 能注册 UDF、写入数据并执行查询
- `raw-throughput.csv` 和 `raw-latency-samples.csv` 能正常产出

## 正式实验

```bash
cd /path/to/iginx-release

export CONDA_BASE=/path/to/miniconda3
export PYTHON_CONDA_ENV=py313_std
export PORTS_TO_CLEAN="6888 7888"

export ARCHITECTURES="legacy adaptive"
export CONCURRENCIES="4 8 16 32 64"
export ROUNDS=3

export ROWS=10000
export COLS=10
export LOOPS=20
export STEADY_INVOCATIONS_PER_THREAD=10
export WARMUP=1

export ADAPTIVE_INITIAL_THREADS=4
export ADAPTIVE_MIN_THREADS=4
export ADAPTIVE_MAX_THREADS=16

export MIXED_DURATION_SECONDS=30
export LIGHT_QUERY_CONCURRENCY=2
export LIGHT_QUERY_LIMIT=256

bash benchmark/run_udf_architecture_matrix.sh
```

## 输出结果

脚本默认输出：

- `benchmark/results/architecture/raw-throughput.csv`
- `benchmark/results/architecture/raw-latency-samples.csv`
- `benchmark/results/architecture/logs/*.log`
- `benchmark/results/architecture/config.properties.bak`

## 本机汇总与画图

把结果拉回本机后执行：

```bash
python test/benchmark/plot_udf_architecture_results.py \
  --throughput-csv /path/to/raw-throughput.csv \
  --latency-csv /path/to/raw-latency-samples.csv \
  --output-dir /path/to/plots
```

输出包括：

- `summary.csv`
- `throughput_vs_concurrency.png`
- `udf_latency_percentiles.png`
- `light_query_p95_vs_concurrency.png`

## 结果解释建议

- 如果 `adaptive` 吞吐显著高于 `legacy`，说明独立 UDF 池有效缓解了高并发下的资源争用。
- 如果 `adaptive` 的 light query P95 明显低于 `legacy`，说明普通查询受 UDF 干扰更小，体现了物理隔离收益。
- 如果高并发下收益没有线性增长，优先从会话池、数据转换、JVM 调度和内存带宽角度解释。

