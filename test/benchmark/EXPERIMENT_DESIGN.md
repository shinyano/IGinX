# IGinX Python UDF Free-Threading Performance Experiment Design

## 1. 文档目的

本文档用于系统说明 IGinX 在 Python UDF 场景下的自由线程性能实验设计，包括实验目标、实验问题、实验流程、参数设计、对比设计、结果指标、结果解释口径以及实验有效性分析。本文档的定位不是简单的操作手册，而是偏向实验报告、论文实验章节和项目技术说明书之间的中间形态，便于后续直接扩展为正式论文或项目汇报材料。

本实验关注的核心问题是：当 IGinX 在执行 Python UDF 时，将底层 Python 解释器从标准 GIL 模式切换为 free-threading 模式后，系统是否能够在多线程并发条件下获得预期的吞吐量提升，以及这种提升在不同类型的 Python UDF 工作负载上是否存在差异。

## 2. 实验背景

### 2.1 问题背景

Python 生态在数据处理和算法扩展方面具有明显优势，因此 Python UDF 成为数据库和数据管理系统中常见的扩展手段之一。然而，标准 CPython 长期受到全局解释器锁 GIL 的影响，在 CPU 密集型纯 Python 代码中，多个线程通常无法真正并行执行 Python 字节码。这会直接限制 Python UDF 在高并发场景下的扩展能力。

随着 Python 3.13 free-threading 解释器的出现，Python 线程在关闭 GIL 后具备了并行执行的可能性。对于 IGinX 而言，这带来了一个重要的工程与研究问题：如果系统内部的 Python UDF 执行链路允许多个 UDF 调用并发调度，那么在 free-threading 解释器下，IGinX 的 Python UDF 能否真正获得显著性能收益。

### 2.2 系统背景

本实验中的被测对象是 IGinX 的 Python UDF 执行能力。实验并不单独评估 Python 解释器本身，而是评估“IGinX + Python UDF + 并发请求 + 指定解释器模式”组成的端到端系统性能。

换句话说，实验结果反映的是以下几部分共同作用后的综合表现：

- IGinX 对 UDF 请求的接收、调度与执行
- UDF 线程池配置
- Python UDF 封装与调用机制
- Python 解释器是否开启 GIL
- UDF 本身的计算特征
- 数据读取、DataFrame 构造与结果回传开销

因此，本实验的结论不应被理解为“free-threading Python 本身快多少”，而应理解为“在 IGinX 的 Python UDF 实际执行链路中，自由线程解释器是否能带来端到端收益，以及收益在多大程度上体现出来”。

## 3. 实验目标

本实验的总体目标是验证 IGinX 在自由线程 Python 解释器下执行 Python UDF 时的并发性能表现，并量化其相对于标准 GIL 解释器的性能差异。

具体而言，实验包含以下几个子目标：

1. 验证在标准 GIL 解释器模式下，IGinX 的 Python UDF 在多线程条件下是否存在明显扩展瓶颈。
2. 验证在 free-threading 解释器模式下，IGinX 的 Python UDF 是否能够随着线程数增加而获得更高吞吐量。
3. 对比两类典型工作负载：
   `pure_python` 与 `numpy_vectorized`。
4. 观察不同工作负载下，free-threading 带来的收益是否一致。
5. 判断系统的性能提升是否符合预期方向，并分析其未达到理想线性加速的潜在原因。

## 4. 研究问题与假设

为了使实验设计更清晰，可以将实验目标进一步抽象为若干研究问题。

### 4.1 研究问题

#### RQ1

在 IGinX 的 Python UDF 执行场景下，free-threading Python 是否能显著提升多线程吞吐量。

#### RQ2

这种提升是否主要体现在纯 Python、CPU 密集型 UDF 上。

#### RQ3

对于大量计算工作由 NumPy 等底层 native 库完成的 UDF，free-threading 的收益是否仍然明显。

#### RQ4

系统在多线程下的扩展效率是否接近线性。如果没有达到线性，瓶颈可能位于哪些环节。

### 4.2 实验假设

结合 Python 执行模型和 UDF 负载特征，可以提出如下假设：

#### H1

对于 `pure_python` 工作负载，标准 GIL 模式在并发度升高时扩展性较弱，而 free-threading 模式将带来明显吞吐提升。

#### H2

对于 `numpy_vectorized` 工作负载，free-threading 仍可能带来收益，但幅度通常小于 `pure_python`，因为其瓶颈不完全受 GIL 限制。

#### H3

即使在 free-threading 模式下，系统整体吞吐量也未必能达到严格线性增长，因为真实端到端路径中还包含线程调度、数据转换、内存访问、会话池和系统资源竞争等额外开销。

## 5. 被测系统与实验组成

本实验由三层组成，分别对应实验执行、数据生成与结果分析三个阶段。

### 5.1 Java benchmark runner

核心类为：

- `cn.edu.tsinghua.iginx.tools.benchmark.PythonUdfThroughputBenchmarkRunner`

该 runner 的职责包括：

- 连接已经启动的 IGinX 实例
- 注册基准测试用的 Python UDF
- 插入测试数据
- 创建并发调用线程
- 统计总调用次数与总耗时
- 将每个实验点的结果追加写入 CSV

从实现逻辑上看，该 runner 对每个 workload 会先做轻量 warmup，然后创建固定数量的客户端线程，并通过 `CountDownLatch` 尽量同步起跑，确保吞吐测试更接近真实的并发压力场景。

### 5.2 服务器端批量执行脚本

核心脚本为：

- `benchmark/run_udf_throughput_matrix.sh`

该脚本用于批量切换实验条件并执行 benchmark。其主要职责包括：

- 在标准解释器和自由线程解释器之间切换 `pythonCMD`
- 将 UDF 线程池大小固定为目标线程数
- 启动和停止 IGinX
- 调用 Java runner 完成实际压测
- 对所有 `mode x threads x round` 组合进行循环
- 输出原始 CSV 和日志

该脚本使整个实验过程标准化、批量化，避免人工逐项改配置导致的不一致。

### 5.3 Python 分析与绘图脚本

分析脚本位于：

- `test/benchmark/plot_udf_throughput_results.py`

该脚本负责：

- 读取原始 CSV
- 过滤失败样本
- 聚合不同实验点的均值与标准差
- 生成 throughput 图和 speedup 图
- 为后续实验分析或论文作图提供基础结果

## 6. 工作负载设计

本实验选择了两类具有代表性的 Python UDF 作为 workload。

### 6.1 `pure_python`

对应脚本：

- `benchmark/udf/throughput_pure_python_udsf.py`

该 workload 的核心逻辑是对输入 DataFrame 中的数值列进行多轮迭代，逐行逐元素转换为浮点数并执行平方累加。其显著特征是：

- 计算主要发生在 Python 层
- 大量执行 Python 循环和 Python 对象访问
- 属于典型的 CPU 密集型纯 Python 工作负载
- 容易受到 GIL 限制

因此，这类 workload 最适合用来验证 free-threading 是否真正打破了纯 Python 多线程执行瓶颈。

### 6.2 `numpy_vectorized`

对应脚本：

- `benchmark/udf/throughput_numpy_udsf.py`

该 workload 首先将数值列转换为 NumPy 数组，然后重复执行向量化平方求和操作。其特征是：

- 计算主要集中在 NumPy 的底层 native 实现
- Python 层参与控制流程，但大部分核心算子不在 Python 字节码层执行
- 更容易受底层库、内存带宽和数组转换成本影响
- 不一定完全受 GIL 约束

因此，该 workload 用于观察：当计算主要下沉到 native 层时，free-threading 解释器还能带来多大收益。

### 6.3 元数据 workload

对应脚本：

- `benchmark/udf/benchmark_metadata_udsf.py`

该 UDF 用于读取 Python 运行时元信息，例如：

- Python 版本号
- 当前运行模式下 GIL 是否开启

其作用不是用于吞吐压测，而是为实验结果增加可验证性，确保写入 CSV 的模式信息与实际运行的解释器状态一致。

## 7. 实验变量设计

一个清晰的实验必须区分自变量、因变量和控制变量。本实验的变量设计如下。

### 7.1 自变量

#### 解释器模式 `mode`

该变量有两个取值：

- `gil`
- `ft`

其中：

- `gil` 表示标准 Python 解释器，GIL 开启
- `ft` 表示 free-threading Python 解释器，GIL 关闭

这是实验最核心的自变量，用于对比自由线程能力是否带来系统级收益。

#### 并发线程数 `threads`

本实验采用多个线程数配置，例如：

- `1`
- `2`
- `4`
- `8`

该变量既代表客户端并发度，也代表服务端 UDF 线程池规模，是实验观察扩展性的第二个核心自变量。

#### 工作负载类型 `workload`

本实验至少包含两类工作负载：

- `pure_python`
- `numpy_vectorized`

该变量用于分析 free-threading 收益是否受 workload 计算特征影响。

### 7.2 因变量

#### 吞吐量 `throughput`

吞吐量定义为单位时间内成功完成的 UDF 调用次数。在 runner 中，其计算方式为：

- `throughput = totalInvocations * 1000 / elapsedMs`

其中：

- `totalInvocations = threads * invocationsPerThread`
- `elapsedMs` 为从所有并发请求同时开始，到全部请求完成的总耗时

吞吐量是本实验的主要评价指标。

#### 扩展比 `speedup`

绘图脚本中的 `speedup` 默认定义为同一 `mode + workload` 下，相对于该组合单线程吞吐的倍数，即：

- `speedup(mode, workload, t) = throughput(mode, workload, t) / throughput(mode, workload, 1)`

该指标用于衡量系统在该模式下随线程数增加的扩展能力。

#### 相对收益 `ft/gil ratio`

为了直接量化自由线程模式相对于标准模式的提升，可以进一步定义：

- `ft/gil ratio(workload, t) = throughput(ft, workload, t) / throughput(gil, workload, t)`

该指标尤其适合展示同一线程数下，自由线程相对于标准解释器的真实收益。

### 7.3 控制变量

为了让不同实验点具有可比性，本实验固定以下参数：

- 数据行数 `rows`
- 数据列数 `cols`
- 每次 UDF 调用中的重复计算轮数 `loops`
- 每个客户端线程执行的调用次数 `invocations_per_thread`
- warmup 轮数
- 被测数据分布方式
- 服务器部署方式
- IGinX 版本与代码版本
- 底层依赖环境

这些变量保持不变，保证性能差异主要由解释器模式、线程数和工作负载类型导致。

## 8. 参数设计与含义解释

本实验中常见参数及其含义如下。

### 8.1 `THREADS`

示例：

```bash
export THREADS="1 2 4 8"
```

含义：

- 表示实验中要测试的并发线程数集合
- 在当前实验设计中，`threads` 同时控制：
  UDF 线程池大小
  客户端并发线程数

设计意图：

- 让服务端可并发执行能力和客户端施加的并发压力保持一致
- 便于直接观察系统随线程数增长的吞吐变化

为什么使用 `1/2/4/8`：

- 这是并发扩展实验中常见的倍增序列
- 可以清晰反映系统从单线程到中高并发阶段的趋势变化
- 便于后续与 CPU 核数、线程池大小和扩展效率进行讨论

### 8.2 `ROUNDS`

示例：

```bash
export ROUNDS=3
```

含义：

- 每个实验点重复执行的轮数

设计意图：

- 降低偶然抖动对结论的影响
- 支持后续计算均值与标准差

为什么通常取 3：

- 3 轮是工程实验中常见的基础重复次数
- 可以在实验耗时和统计稳定性之间取得平衡
- 如果后续需要更严谨统计分析，可以扩展为 5 轮或更多

### 8.3 `ROWS`

示例：

```bash
export ROWS=10000
```

含义：

- 每次实验插入到 IGinX 中的时间序列数据行数

设计意图：

- 控制单次 UDF 读取的数据规模
- 保证每次查询都需要处理足够的数据量，避免 workload 过轻导致差异不明显

参数影响：

- `ROWS` 太小，可能让固定开销主导实验结果
- `ROWS` 太大，则可能让数据读取和内存压力掩盖解释器层面的收益

`10000` 是一个折中设置，既能让 workload 保持一定计算量，又不会让单次实验耗时过长。

### 8.4 `COLS`

示例：

```bash
export COLS=10
```

含义：

- 每一行包含的数值列数

设计意图：

- 控制 UDF 每次处理的宽表程度
- 与 `ROWS` 一起共同决定输入数据规模

参数影响：

- `COLS` 越大，DataFrame 和 NumPy array 的列宽越高
- 这会增加逐元素计算量，也会增加列访问与内存访问开销

### 8.5 `LOOPS`

示例：

```bash
export LOOPS=20
```

含义：

- 每次 UDF 调用中，对同一批输入数据重复执行计算的次数

设计意图：

- 放大计算量，让吞吐测试更聚焦于计算阶段而不是调用固定开销

参数影响：

- `LOOPS` 越大，单次 UDF 调用越重
- 对 `pure_python` 而言，能更明显地放大 GIL 的影响
- 对 `numpy_vectorized` 而言，能更明显地观察底层向量化计算的扩展特性

### 8.6 `INVOCATIONS_PER_THREAD`

示例：

```bash
export INVOCATIONS_PER_THREAD=10
```

含义：

- 每个客户端线程发起的 UDF 调用次数

设计意图：

- 避免只测一次调用导致偶然误差较大
- 让单个实验点的吞吐统计更加稳定

在当前实验设计中：

- 总调用数为 `threads * invocations_per_thread`
- 当 `threads=8` 且 `invocations_per_thread=10` 时，总调用数为 `80`

### 8.7 `WARMUP`

示例：

```bash
export WARMUP=1
```

含义：

- 正式测量前的预热轮数

设计意图：

- 避免首次调用中的额外开销直接影响正式结果
- 减少动态初始化、缓存建立或运行时装载对正式测量的扰动

在当前 runner 实现中，warmup 并不会使用完整 `rows` 和完整 `loops`，而是采用较轻量的预热输入，这样既能起到预热作用，又不会显著增加实验时长。

## 9. 对比设计

### 9.1 模式对比

最核心的对比是：

- 标准解释器 `gil`
- 自由线程解释器 `ft`

该对比回答的问题是：在其他条件保持一致时，仅替换 Python 解释器执行模式，IGinX 的 Python UDF 吞吐是否发生显著变化。

### 9.2 线程扩展对比

在每个模式内部，采用不同线程数进行测试，观察：

- 吞吐量是否随线程数增加而持续提升
- speedup 是否接近线性增长
- 高线程数下是否出现扩展停滞或退化

该对比回答的是“系统扩展性”问题。

### 9.3 工作负载对比

对比：

- `pure_python`
- `numpy_vectorized`

该对比回答的问题是：free-threading 的收益是否受 workload 性质影响，以及这种收益主要在何类 UDF 上表现更明显。

### 9.4 单线程与多线程对比

单线程结果尤其重要，因为它为后续的 speedup 提供基线。一般会出现如下现象：

- `ft` 在单线程下未必快于 `gil`
- `ft` 的价值通常主要体现在多线程并行时

因此，不能仅凭单线程表现评价 free-threading 的价值，必须结合中高并发阶段一起分析。

## 10. 实验流程设计

本实验采用“逐实验点重启 IGinX”的方式执行。对每个 `mode x threads x round` 组合，整体流程如下。

### 10.1 运行前准备

1. 解析目标 Python 解释器路径。
2. 校验对应环境能导入 `numpy`、`pandas`、`iginx_udf` 以及 pemja 相关本地库。
3. 备份 `conf/config.properties`。

### 10.2 配置实验点

对当前实验点，脚本会更新：

- `pythonCMD`
- `udfPoolEnabled=true`
- `udfPoolMinThreads=threads`
- `udfPoolMaxThreads=threads`
- `udfPoolInitialThreads=threads`

这样做的意义在于：

- 把 Python 解释器模式切换到目标环境
- 把 UDF 执行线程池固定到目标并发规模
- 避免线程池自动伸缩影响可比性

### 10.3 启动与执行

1. 启动 IGinX。
2. Java runner 连接 IGinX。
3. 注册基准测试用 UDF。
4. 清理旧数据并插入新测试数据。
5. 执行元数据查询，记录 Python 版本与 GIL 状态。
6. 对每个 workload 先进行 warmup，再进行正式 benchmark。
7. 将结果写入原始 CSV。

### 10.4 清理与下一轮

1. 停止 IGinX。
2. 清理残留监听端口。
3. 进入下一轮实验。
4. 全部实验结束后恢复原始配置文件。

这种设计的优点是：

- 每个实验点的配置边界清晰
- 模式切换和线程池切换不会相互污染
- 更适合用于严谨对比实验

其代价是：

- 整体实验总时长更长
- 频繁重启可能引入额外固定开销

不过，由于吞吐统计只覆盖正式 benchmark 阶段，因此启动与停止开销不会直接计入吞吐量本身。

## 11. 指标定义与结果整理方法

### 11.1 原始结果字段解释

原始 CSV 一般包含以下字段：

- `mode`
- `round`
- `workload`
- `threads`
- `rows`
- `cols`
- `loopsPerInvocation`
- `invocationsPerThread`
- `totalInvocations`
- `elapsedMs`
- `throughput`
- `pythonVersion`
- `gilEnabled`
- `status`
- `error`

其中：

- `mode` 表示解释器模式
- `round` 表示当前重复轮次
- `workload` 表示 UDF 类型
- `threads` 表示并发线程数
- `elapsedMs` 表示总耗时
- `throughput` 表示吞吐量
- `gilEnabled` 用于核实实验实际处于 GIL 或 free-threading 状态
- `status` 和 `error` 用于定位失败样本

### 11.2 数据聚合方式

正式分析时，建议只使用 `status=ok` 的样本，并按如下维度分组：

- `mode`
- `workload`
- `threads`

对每组样本计算：

- 平均吞吐量
- 标准差
- 相对单线程 speedup
- `ft/gil` 吞吐比

### 11.3 推荐图表

建议至少输出以下图表：

1. `throughput` 折线图  
   展示不同模式和 workload 下，吞吐量随线程数变化的趋势。

2. `speedup` 折线图  
   展示每个模式在各 workload 上相对自身单线程的扩展性。

3. `ft/gil ratio` 柱状图  
   直接比较同一线程数下，自由线程模式相对于标准模式的提升倍数。

4. 按 workload 拆分的 throughput 对比图  
   将 `pure_python` 和 `numpy_vectorized` 分开画图，以增强可读性。

## 12. 参数设计背后的考虑

### 12.1 为什么选择端到端测试而不是单独测 Python 函数

因为实验目标是评估 IGinX 的 Python UDF 实际执行能力，而不是孤立地评估 Python 解释器微基准。若直接在 Python 进程内测函数耗时，无法覆盖：

- UDF 注册与执行链路
- SessionPool 并发调用
- DataFrame 构造
- 系统侧线程调度
- 结果回传

端到端实验虽然更复杂，但结论更接近真实系统使用场景。

### 12.2 为什么同时保留 `pure_python` 和 `numpy_vectorized`

如果只测 `pure_python`，容易得出“free-threading 很有效”的结论，但这一结论可能仅适用于受 GIL 严重限制的 workload。  
如果只测 `numpy_vectorized`，又可能低估 free-threading 的价值，因为该类 workload 的主要计算并不都发生在 Python 字节码层。

同时保留两类 workload，能够更全面地回答：

- 自由线程在哪类 UDF 上最有效
- 自由线程收益在真实混合场景中可能处于什么范围

### 12.3 为什么将客户端并发度与 UDF 线程池大小同步

这是为了降低实验设计复杂度，使一个参数 `threads` 同时代表：

- 外部压力强度
- 内部执行并行度

这样可以更直接地观察“当系统开放 N 路并发执行能力时，吞吐是否随 N 增长”。  
后续如果需要更细粒度研究，也可以进一步拆分为：

- 固定客户端并发，改变 UDF 线程池
- 固定 UDF 线程池，改变客户端并发

但那属于后续更细的补充实验。

## 13. 结果分析建议

### 13.1 如何判断实验是否达到预期

如果观察到以下现象，则可以认为实验基本验证了自由线程的价值：

1. `pure_python` 在 `gil` 模式下随着线程数增长几乎没有扩展，甚至出现退化。
2. `pure_python` 在 `ft` 模式下随着线程数增长吞吐显著提升。
3. 同一线程数下，尤其在 `4` 和 `8` 线程时，`ft` 的吞吐明显高于 `gil`。
4. `numpy_vectorized` 也有一定提升，但提升弱于 `pure_python`。

### 13.2 如何解读“单线程不占优”

如果出现 `ft` 在 1 线程下不如 `gil` 的情况，不应直接否定 free-threading 的价值。这是合理现象，原因可能包括：

- free-threading 运行时引入了额外同步或对象管理成本
- 单线程时并行能力尚未发挥
- 该模式的收益主要体现在多线程阶段

因此，对 free-threading 的评价重点应放在中高并发场景。

### 13.3 如何解读“未达到线性加速”

即使 `ft` 吞吐明显提升，也不意味着一定能达到理想的 N 倍扩展。可能限制因素包括：

- IGinX 侧调度开销
- Python UDF 封装开销
- DataFrame 构造与数据复制
- NumPy 数组转换成本
- 内存带宽争用
- CPU cache 效果下降
- SessionPool 和网络路径开销
- 底层运行时资源竞争

因此，实验中更重要的是验证：

- 是否突破了 GIL 模式下的扩展瓶颈
- 是否获得了稳定、持续、方向正确的收益

## 14. 实验有效性与威胁分析

为了让实验结论更可信，需要说明潜在的有效性威胁。

### 14.1 内部有效性

潜在威胁包括：

- 压测程序自身并发逻辑存在缺陷
- 某些失败样本混入统计
- 不同轮次之间配置未完全恢复
- 启动环境与实际执行环境不一致

降低风险的方式包括：

- 通过元数据 UDF 记录 `gilEnabled`
- 只统计 `status=ok` 的样本
- 对每个实验点重启 IGinX
- 使用多轮重复取平均

### 14.2 构造有效性

吞吐量是系统级指标，但它不等价于“纯 Python 解释器计算速度”。因此，在表述结论时应强调：

- 实验衡量的是 IGinX Python UDF 端到端吞吐
- 不应把结果直接解释为 CPython 内核级 microbenchmark

### 14.3 外部有效性

实验采用的是两类代表性 workload，但并不覆盖所有 UDF 类型。因此结论更适用于：

- 纯 Python CPU 密集型 UDF
- 含向量化计算的数值型 UDF

对于 IO 密集型 UDF、模型推理型 UDF、复杂依赖型 UDF，仍需要进一步实验验证。

## 15. 复现实验的推荐表述

如果需要在论文或技术报告中描述实验过程，可以使用类似如下表述：

“我们在 IGinX 中构建了一套 Python UDF 端到端吞吐量基准测试框架。实验通过批量脚本在标准 CPython 3.13 与 free-threading CPython 3.13 之间切换运行时，并同步调整 UDF 线程池大小。对每个实验点，系统会重新启动 IGinX，注册测试用 UDF，写入固定规模数据，然后由 Java benchmark runner 以多线程方式并发发起 UDF 查询。实验记录每组配置下的总调用次数、总耗时和吞吐量，并对不同轮次结果取平均。我们选取了两类 workload：一类为受 GIL 显著限制的纯 Python 计算型 UDF，另一类为以 NumPy 向量化运算为主的 UDF，以观察 free-threading 收益在不同负载类型上的差异。”

## 16. 后续可扩展实验

当前实验已经能够回答自由线程是否带来收益，但如果需要形成更完整的实验章节，建议后续补充以下扩展实验：

### 16.1 更细粒度线程配置

可加入：

- `threads=16`
- `threads=32`

观察在更高并发度下是否继续增长，或者开始饱和。

### 16.2 分离客户端并发和 UDF 线程池

例如：

- 固定客户端并发为 8，改变 UDF 线程池为 1/2/4/8
- 固定 UDF 线程池为 8，改变客户端并发为 1/2/4/8/16

这有助于判断瓶颈位于请求压力侧还是执行资源侧。

### 16.3 增加更多 workload

例如：

- 轻量级 Python 标量 UDF
- IO 密集型 UDF
- pandas 主导型 UDF
- 模型推理型 UDF

### 16.4 加入资源利用率观测

建议同步采集：

- CPU 利用率
- 单核/多核占用情况
- 内存占用
- 上下文切换

这会让“为什么没有线性加速”的分析更有支撑。

## 17. 推荐的结果撰写结构

后续你在正式论文或报告中，可以按如下结构撰写实验部分：

1. 实验目标  
   说明希望验证自由线程 Python 对 IGinX Python UDF 并发性能的影响。

2. 实验环境  
   说明硬件环境、操作系统、Python 版本、IGinX 版本、依赖环境。

3. 实验设计  
   说明两个模式、两个 workload、多个线程数、固定参数以及重复次数。

4. 指标定义  
   说明 throughput、speedup、ft/gil ratio。

5. 实验结果  
   结合表格与图分别展示 `pure_python` 和 `numpy_vectorized` 的结果。

6. 结果分析  
   解释为什么 `pure_python` 收益更显著，为什么 `numpy_vectorized` 收益相对有限，以及为什么没有完全线性扩展。

7. 小结  
   总结自由线程在 IGinX Python UDF 中是否实现了预期收益。

## 18. 本实验当前结论模板

在不绑定某一份具体数据文件的前提下，可以先准备如下通用结论模板，后续只需把具体数值替换进去即可：

“实验结果表明，在 IGinX 的 Python UDF 场景中，自由线程 Python 解释器能够显著提升多线程条件下的吞吐量。对于以 Python 字节码执行为主的 `pure_python` 工作负载，标准 GIL 模式下吞吐量随线程数增加几乎不扩展，而 free-threading 模式下吞吐量随线程数显著上升，说明系统已经能够利用自由线程解释器实现更高程度的并行执行。对于 `numpy_vectorized` 工作负载，自由线程同样带来了吞吐提升，但幅度低于 `pure_python`，说明该类 workload 的瓶颈不只受 GIL 限制，还受到底层 native 计算和系统资源开销的共同影响。总体上，自由线程模式在 IGinX Python UDF 中达到了预期方向上的性能提升，但距离理想线性加速仍存在一定差距。”

## 19. 结语

本实验设计的核心价值在于，它不是一个只针对 Python 函数本身的微基准，而是一套面向 IGinX Python UDF 实际执行路径的端到端性能评估方法。通过同时对比标准 GIL 模式与 free-threading 模式、同时设置纯 Python 和 NumPy 两类 workload，并在多个线程数下进行重复实验，本设计能够较为系统地回答“自由线程 Python 是否真正提升了 IGinX Python UDF 的并发性能”这一问题。

后续只要补充更完整的实验环境信息、具体结果表格与图表，以及对异常样本和高并发瓶颈的进一步分析，就可以将本文档直接扩展为正式实验章节。
