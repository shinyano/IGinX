# Python UDF 吞吐量扩展性实验

这套实验工具按“本机编译，服务器只运行”来设计，分为三层：

1. `core` 内的 Java runner
   类名：`cn.edu.tsinghua.iginx.tools.benchmark.PythonUdfThroughputBenchmarkRunner`
   作用：连接已启动的 IGinX，注册 benchmark UDF、写入测试数据、并发执行查询、输出原始 csv。

2. 服务器侧脚本
   路径：发行包内 `benchmark/run_udf_throughput_matrix.sh`
   作用：切换 `pythonCMD` 和 UDF 线程池线程数，启动/停止 IGinX，批量调用 Java runner。

3. 本机分析脚本
   路径：`test/benchmark/plot_udf_throughput_results.py`
   作用：读取服务器回传的 `raw-results.csv`，聚合均值并输出吞吐量图和 speedup 图。

## 服务器端目录结构

当你在本机完成打包并把产物传到服务器后，发行包里会包含：

- `lib/*`
- `conf/config.properties`
- `sbin/start_iginx.sh`
- `benchmark/run_udf_throughput_matrix.sh`
- `benchmark/udf/benchmark_metadata_udsf.py`
- `benchmark/udf/throughput_pure_python_udsf.py`
- `benchmark/udf/throughput_numpy_udsf.py`

## 服务器端执行方式

### 约定的 conda 环境

你当前服务器上的两套环境是：

- 标准模式：`conda activate py313_std`
- 自由线程模式：`conda activate py313_ft`

脚本已经按这两个环境名做了默认适配：

- `GIL_CONDA_ENV=py313_std`
- `FT_CONDA_ENV=py313_ft`

也就是说，如果你的环境名不变，通常不需要再显式设置 `GIL_PYTHON` 和 `FT_PYTHON`。

### 前置条件

使用你自己的部署/传包流程完成以下前置步骤：

1. 在本机完成编译和打包。
2. 把打包后的 IGinX 运行目录传到服务器。
3. 服务器上已经安装并可用 `conda`。
4. 服务器上已经创建好：
   `py313_std`
   `py313_ft`
5. 服务器上能正常启动底层存储、ZooKeeper 以及 IGinX。

### 最简单跑法

假设你已经进入服务器上的发行包根目录：

```bash
cd /path/to/iginx-release
bash benchmark/run_udf_throughput_matrix.sh
```

如果脚本所在 shell 里拿不到 `conda` 命令，那么手工指定 `CONDA_BASE` 即可：

```bash
cd /path/to/iginx-release
export CONDA_BASE=/path/to/miniconda3
bash benchmark/run_udf_throughput_matrix.sh
```

### 推荐的完整跑法

```bash
cd /path/to/iginx-release

export CONDA_BASE=/path/to/miniconda3
export GIL_CONDA_ENV=py313_std
export FT_CONDA_ENV=py313_ft
export PORTS_TO_CLEAN="6888 7888"

export MODES="gil ft"
export THREADS="1 2 4 8"
export ROUNDS=3

export ROWS=10000
export COLS=10
export LOOPS=20
export INVOCATIONS_PER_THREAD=10
export WARMUP=1

bash benchmark/run_udf_throughput_matrix.sh
```

### 如果你想先做一次 smoke test

先只跑 2 个模式、1 个线程数、1 轮：

```bash
cd /path/to/iginx-release
export CONDA_BASE=/path/to/miniconda3
export THREADS="1"
export ROUNDS=1
bash benchmark/run_udf_throughput_matrix.sh
```

这样可以先确认：

- 两个 conda 环境都能被正确解析
- IGinX 能正常启动
- runner 能成功注册 UDF、写数据、执行查询
- 原始 csv 能正常产出

如果你发现 IGinX 上一次退出后仍残留监听端口，可以显式指定要清理的端口：

```bash
export PORTS_TO_CLEAN="6888 7888"
```

脚本会在每轮启动前和停止后自动检测这些端口上的监听进程，并尝试清理残留进程。

### 如果你使用自己的启动/停止脚本

默认情况下，脚本会直接调用：

- `sbin/start_iginx.sh`

如果你的服务器上实际使用的是你自己封装好的启动/停止方式，可以这样覆盖：

```bash
export START_CMD="/your/custom/start-command"
export STOP_CMD="/your/custom/stop-command"
```

注意：

- 只要设置了 `START_CMD`，就必须同时设置 `STOP_CMD`
- 你的 `START_CMD` 需要负责把 IGinX 启起来
- 你的 `STOP_CMD` 需要负责把 IGinX 干净停掉

### 脚本实际做了什么

对每个实验点，也就是每个：

- `mode`
- `threads`
- `round`

脚本都会执行以下动作：

1. 把 `conf/config.properties` 备份。
2. 修改 `pythonCMD` 为对应 conda 环境里的 `bin/python`。
3. 固定：
   `udfPoolMinThreads`
   `udfPoolMaxThreads`
   `udfPoolInitialThreads`
   为目标线程数。
4. 启动 IGinX。
5. 调用 Java runner 执行一次端到端 benchmark。
6. 停止 IGinX。
7. 进入下一个实验点。
8. 全部结束后恢复原始 `config.properties`。
9. 每轮启动前和停止后自动清理 `PORTS_TO_CLEAN` 中残留的监听进程。

### 输出结果

脚本默认会输出：

- 原始结果：`benchmark/results/raw-results.csv`
- 每轮日志：`benchmark/results/logs/*.log`
- 运行期间的配置备份：`benchmark/results/config.properties.bak`

## 本机汇总与画图

把 `raw-results.csv` 拉回本机后执行：

```bash
python test/benchmark/plot_udf_throughput_results.py \
  --input-csv /path/to/raw-results.csv \
  --output-dir /path/to/plots
```

输出包括：

- `summary.csv`
- `throughput.png`
- `speedup.png`

## 傻瓜式操作手册

下面按最省事的方式给出完整流程。

### 第 1 步：本机编译

在本机仓库根目录执行你自己的打包流程，目标是产出可直接运行的 IGinX 目录。

至少要确保最终产物里包含：

- `lib/*`
- `conf/config.properties`
- `sbin/start_iginx.sh`
- `benchmark/run_udf_throughput_matrix.sh`
- `benchmark/udf/benchmark_metadata_udsf.py`
- `benchmark/udf/throughput_pure_python_udsf.py`
- `benchmark/udf/throughput_numpy_udsf.py`

### 第 2 步：把产物传到服务器

把整个运行目录传到服务器，例如：

```bash
/data/iginx-benchmark-run/
```

### 第 3 步：在服务器确认 conda 环境

你可以手工检查一次：

```bash
conda activate py313_std
python -V
python -c "import sys; print(getattr(sys, '_is_gil_enabled', lambda: 'unknown')())"

conda activate py313_ft
python -V
python -c "import sys; print(getattr(sys, '_is_gil_enabled', lambda: 'unknown')())"
```

预期：

- `py313_std` 返回 `True` 或等价的 GIL 开启状态
- `py313_ft` 返回 `False` 或等价的 GIL 关闭状态

### 第 4 步：先做一次最小验证

进入服务器运行目录：

```bash
cd /data/iginx-benchmark-run
export CONDA_BASE=/path/to/miniconda3
export THREADS="1"
export ROUNDS=1
bash benchmark/run_udf_throughput_matrix.sh
```

如果这一步成功，说明整套链路已经打通。

### 第 5 步：跑正式实验

把参数改成正式值：

```bash
cd /data/iginx-benchmark-run

export CONDA_BASE=/path/to/miniconda3
export GIL_CONDA_ENV=py313_std
export FT_CONDA_ENV=py313_ft

export MODES="gil ft"
export THREADS="1 2 4 8"
export ROUNDS=3

export ROWS=10000
export COLS=10
export LOOPS=20
export INVOCATIONS_PER_THREAD=10
export WARMUP=1

bash benchmark/run_udf_throughput_matrix.sh
```

### 第 6 步：取回结果

正式实验结束后，至少取回：

- `benchmark/results/raw-results.csv`
- `benchmark/results/logs/`

### 第 7 步：本机画图

把 `raw-results.csv` 放到本机后执行：

```bash
python test/benchmark/plot_udf_throughput_results.py \
  --input-csv /path/to/raw-results.csv \
  --output-dir /path/to/plots
```

### 第 8 步：查看最终产物

你会得到：

- `summary.csv`
- `throughput.png`
- `speedup.png`

## 结果解释建议

- `pure_python`
  预期在 GIL 模式下扩展性弱，在自由线程模式下更接近线性增长。

- `numpy_vectorized`
  预期在两种模式下差异较小，更多受底层 C 实现和系统资源影响。

