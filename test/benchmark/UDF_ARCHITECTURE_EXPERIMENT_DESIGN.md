# IGinX 自适应 Python UDF 线程池架构对比实验设计

## 1. 文档目的

本文档用于说明 IGinX 中“旧架构 vs 新架构”的 Python UDF 执行对比实验设计。本文档服务于硕士论文中的工程设计实验评估部分，重点回答如下问题：

- 当关闭自适应 UDF 专用线程池时，系统在高并发 UDF 负载下的吞吐与尾延迟表现如何。
- 当启用独立 UDF 线程池与自适应调度后，系统是否在稳态并发场景下获得更高吞吐。
- 在 UDF 重负载背景下，轻量级非 UDF 查询是否仍能保持较低延迟，从而体现资源隔离的价值。

该文档延续现有自由线程实验的表达风格，但实验变量改为 `architecture=legacy/adaptive`，不覆盖已有自由线程实验代码与脚本。

## 2. 实验背景

### 2.1 系统背景

IGinX 在 Python UDF 执行路径中引入了自适应 UDF 专用线程池，其设计目标包括：

- 将 Python UDF 从通用内存任务线程池中剥离，实现物理隔离。
- 根据队列长度、活跃线程比例和 CPU 利用率动态扩缩线程池。
- 在线程退出时回收线程本地 Python 解释器，避免资源泄漏。

设计细节可参考：

- `core/src/main/java/cn/edu/tsinghua/iginx/engine/physical/udf/DESIGN.md`

### 2.2 对比对象定义

本实验中的两种架构定义如下：

- `legacy`
  - 通过配置 `udfPoolEnabled=false` 关闭自适应 UDF 池。
  - `PyUDF.invokePyUDF()` 在调用方内存任务线程中同步执行。
  - 这对应设计文档中的回退语义，可视为改造前的执行路径。
- `adaptive`
  - 通过配置 `udfPoolEnabled=true` 启用独立 UDF 池。
  - Python UDF 任务提交到 `AdaptiveUDFExecutor` 管理的专用线程池。
  - 线程池初始线程数固定为 4，最大线程数取 `CPU * 2` 或脚本显式设置值。

因此，本实验不是比较两个历史代码版本，而是在同一代码基线下，通过配置切换两种执行架构。

## 3. 实验目标

本实验的总体目标是验证自适应 UDF 专用线程池在工程层面带来的系统收益，并量化其对吞吐、尾延迟和轻量查询隔离性的影响。

具体子目标如下：

1. 比较旧架构与新架构在稳定并发 UDF 负载下的吞吐量差异。
2. 比较两种架构在单次 UDF 请求上的 P50 / P95 / P99 延迟差异。
3. 在 UDF 压力背景下，比较轻量级非 UDF 查询的延迟退化程度。
4. 为论文中的“独立线程池隔离效果”和“自适应调度有效性”提供实验依据。

## 4. 研究问题与假设

### 4.1 研究问题

#### RQ1

在稳定并发 UDF 负载下，`adaptive` 架构是否能显著提升系统吞吐量。

#### RQ2

在中高并发场景中，`adaptive` 架构是否能改善 UDF 请求的尾延迟表现。

#### RQ3

在 UDF 查询持续占用执行资源时，`adaptive` 架构是否能明显降低轻量级非 UDF 查询的延迟恶化。

#### RQ4

`adaptive` 架构的收益是否主要来自任务隔离，而不仅仅是线程数增加。

### 4.2 实验假设

基于设计文档中的架构分析，可提出如下假设：

#### H1

在 `legacy` 架构下，随着并发度提高，UDF 查询吞吐量会较早进入饱和甚至退化，因为 UDF 与通用内存任务共享执行资源。

#### H2

在 `adaptive` 架构下，系统在高并发阶段能够保持更高吞吐量，因为 UDF 任务与普通任务分离后，通用线程池不再直接承受 Python UDF 的长期占用。

#### H3

在混合负载场景下，`legacy` 架构中的轻量查询 P95 延迟将明显上升，而 `adaptive` 架构下轻量查询延迟恶化程度较小。

#### H4

即使 `adaptive` 架构吞吐量更高，也不一定表现为理想线性扩展，因为系统仍然受到数据读取、DataFrame 构造、会话池竞争、线程切换和解释器调用开销的影响。

## 5. 被测系统与实验组成

本实验采用与现有自由线程 benchmark 一致的三层组织方式，但实现为一套独立文件。

### 5.1 Java Benchmark Runner

核心类：

- `cn.edu.tsinghua.iginx.tools.benchmark.AdaptiveUdfArchitectureBenchmarkRunner`

职责包括：

- 连接已启动的 IGinX。
- 注册本实验专用的 benchmark UDF。
- 写入固定规模测试数据。
- 执行稳态 UDF 吞吐场景。
- 执行混合负载场景。
- 记录实验点级别的汇总结果。
- 记录单请求延迟样本，供后续计算分位数。

### 5.2 服务器端批量执行脚本

核心脚本：

- `benchmark/run_udf_architecture_matrix.sh`

职责包括：

- 切换 `architecture=legacy/adaptive`
- 设置 `pythonCMD`
- 批量更新 `udfPoolEnabled` 与相关线程池参数
- 启停 IGinX
- 调用 Java runner
- 产出原始 CSV 与日志

### 5.3 本机分析与绘图脚本

核心脚本：

- `test/benchmark/plot_udf_architecture_results.py`

职责包括：

- 聚合不同轮次的吞吐结果
- 计算 UDF / light query 的 P50 / P95 / P99
- 输出 `summary.csv`
- 输出论文可直接使用的折线图

## 6. 工作负载设计

### 6.1 UDF 负载

UDF 负载采用 CPU 密集型纯 Python workload，对输入数据逐元素执行重复平方累加操作。其特征为：

- Python 层循环多
- 计算时间明显
- 容易在高并发场景下放大调度与隔离差异

这类 workload 更适合验证：

- UDF 专用线程池是否提升稳态吞吐
- 旧架构中 UDF 是否会挤压普通任务资源

### 6.2 轻量级查询负载

轻量级查询采用纯投影 / 过滤形式，例如：

```sql
SELECT * FROM bench.arch WHERE key < 256;
```

该查询不触发 Python UDF，仅用于评估在 UDF 背景负载下普通查询延迟是否受到影响。

### 6.3 元数据 workload

通过专用元数据 UDF 记录：

- Python 版本号
- 当前运行时 GIL 状态

虽然本实验不以 `gil/ft` 为自变量，但保留该元数据字段可以帮助确认远程运行环境一致性。

## 7. 实验变量设计

### 7.1 自变量

#### 架构类型 `architecture`

取值：

- `legacy`
- `adaptive`

这是本实验最核心的自变量。

#### 并发查询数 `concurrency`

正式实验建议取值：

- `4`
- `8`
- `16`
- `32`
- `64`

该变量表示 UDF 压测客户端的并发线程数。

#### 场景类型 `scenario`

取值：

- `steady_udf`
- `mixed_projection`

其中：

- `steady_udf` 用于测量纯 UDF 并发吞吐和 UDF 延迟分布。
- `mixed_projection` 用于在 UDF 背景压力下测量轻量查询延迟。

### 7.2 因变量

#### 吞吐量 `throughput`

在 `steady_udf` 场景中，吞吐量定义为：

- `throughput = totalInvocations * 1000 / elapsedMs`

在 `mixed_projection` 场景中，UDF 背景流量同样记录吞吐量，但其主要作用是证明 light query 测量窗口内存在稳定 UDF 压力。

#### 延迟分位数

对每类请求记录单次延迟样本，后处理计算：

- `P50`
- `P95`
- `P99`

其中：

- UDF 查询的 P50 / P95 / P99 用于反映主负载的服务质量。
- light query 的 P95 尤其关键，用于评估尾延迟隔离效果。

### 7.3 控制变量

为保证实验点可比性，固定以下参数：

- 数据规模：`rows=10000`
- 列数：`cols=10`
- 每次 UDF 调用内部计算轮数：`loops=20`
- 稳态场景中每线程请求次数：`steadyInvocationsPerThread`
- 混合场景持续时间：`mixedDurationSeconds`
- 轻量查询并发度：`lightQueryConcurrency`
- light query 读取范围：`key < 256`
- Python 运行环境
- IGinX 代码版本
- 服务器硬件与部署方式

## 8. 参数设计与解释

### 8.1 为什么使用 `4 / 8 / 16 / 32 / 64`

- 这些并发度覆盖中低并发到高并发阶段。
- 能较清晰观察旧架构何时出现饱和。
- 也能观察新架构在高并发下是否继续保持收益。

### 8.2 为什么数据规模使用 `10K x 10`

- 规模足够大，可避免固定调用开销主导结果。
- 规模又不至于让单次实验耗时过长。
- 与已有自由线程实验保持一致，方便论文横向叙事。

### 8.3 为什么混合负载使用小范围 light query

- 目的是测“普通查询是否被拖慢”，不是让 light query 自己成为系统主负载。
- 使用较小扫描范围可以凸显调度隔离问题，而不是数据量问题。

### 8.4 为什么每个实验点都重启 IGinX

- 避免 `AdaptiveUDFExecutor` 单例持有旧配置。
- 确保 `legacy` 与 `adaptive` 的切换边界清晰。
- 降低配置污染导致的实验偏差。

## 9. 对比设计

### 9.1 稳态吞吐对比

对每个 `concurrency` 比较：

- `legacy` 的 UDF 吞吐量
- `adaptive` 的 UDF 吞吐量

这回答“新架构是否更快”。

### 9.2 UDF 尾延迟对比

对每个 `concurrency` 比较：

- `legacy` 的 UDF P50 / P95 / P99
- `adaptive` 的 UDF P50 / P95 / P99

这回答“新架构是否更稳”。

### 9.3 混合负载隔离对比

在同样的 UDF 并发背景下，比较：

- `legacy` 的 light query P95
- `adaptive` 的 light query P95

这回答“新架构是否保护了普通查询”。

## 10. 实验流程设计

### 10.1 运行前准备

1. 在本机完成 `core` 模块编译与 assembly 打包。
2. 将发行包上传到服务器。
3. 在服务器准备好 Python 环境及依赖。
4. 确认底层存储、ZooKeeper、IGinX 运行依赖可用。

### 10.2 配置实验点

服务器脚本在每个实验点会：

1. 设置 `pythonCMD`
2. 当 `architecture=legacy`：
   - `udfPoolEnabled=false`
3. 当 `architecture=adaptive`：
   - `udfPoolEnabled=true`
   - `udfPoolInitialThreads=4`
   - `udfPoolMinThreads=4`
   - `udfPoolMaxThreads=CPU*2` 或预设值

### 10.3 执行稳态场景

1. 启动 IGinX
2. 注册 benchmark UDF
3. 写入固定测试数据
4. 预热 UDF 查询
5. 发起 `concurrency` 个并发 UDF 查询线程
6. 每个线程执行固定次数请求
7. 记录总耗时、总调用数和单次延迟样本

### 10.4 执行混合场景

1. 在同一个实验点中继续保持 UDF 背景流量
2. 同时启动固定数量的 light query 线程
3. 在持续时间窗口内分别记录：
   - UDF 请求吞吐
   - light query 单次延迟

### 10.5 清理与下一轮

1. 停止 IGinX
2. 清理残留端口
3. 进入下一实验点
4. 全部结束后恢复原始配置

## 11. 原始结果字段设计

### 11.1 汇总 CSV

建议输出字段：

- `architecture`
- `round`
- `scenario`
- `concurrency`
- `rows`
- `cols`
- `loopsPerInvocation`
- `steadyInvocationsPerThread`
- `mixedDurationSeconds`
- `lightQueryConcurrency`
- `lightQueryLimit`
- `totalInvocations`
- `elapsedMs`
- `throughput`
- `pythonVersion`
- `gilEnabled`
- `status`
- `error`

### 11.2 延迟样本 CSV

建议输出字段：

- `architecture`
- `round`
- `scenario`
- `workloadType`
- `concurrency`
- `requestId`
- `latencyMs`

其中：

- `workloadType=udf` 表示 UDF 请求
- `workloadType=light` 表示轻量级非 UDF 查询

## 12. 数据聚合与图表建议

### 12.1 数据聚合方式

对 `status=ok` 的汇总记录，按以下维度分组：

- `architecture`
- `scenario`
- `concurrency`

计算：

- 吞吐均值
- 吞吐标准差

对延迟样本按轮次先计算：

- `p50`
- `p95`
- `p99`

再按实验点对各轮次结果取平均。

### 12.2 推荐图表

建议至少输出以下图表：

1. `throughput_vs_concurrency.png`
   - X 轴：并发度
   - Y 轴：吞吐量
   - 两条线：`legacy` / `adaptive`
2. `udf_latency_percentiles.png`
   - 展示 UDF 请求在不同并发度下的 P50 / P95 / P99
3. `light_query_p95_vs_concurrency.png`
   - X 轴：并发度
   - Y 轴：light query P95
   - 两条线：`legacy` / `adaptive`
4. `summary.csv`
   - 用于论文表格与后续统计分析

## 13. 结果分析建议

### 13.1 预期现象

若设计成立，预期会观察到：

1. `adaptive` 在中高并发阶段的吞吐高于 `legacy`
2. `adaptive` 的 UDF P95 / P99 低于 `legacy`
3. `legacy` 下 light query 的 P95 随 UDF 并发升高显著恶化
4. `adaptive` 下 light query P95 增幅相对平缓

### 13.2 如何解释吞吐未线性增长

即使 `adaptive` 优于 `legacy`，也可能无法达到理想线性扩展，原因包括：

- Java 线程调度开销
- DataFrame 构造与数据复制开销
- Python 调用桥接开销
- SessionPool 竞争
- CPU cache 与内存带宽争用
- 高并发阶段的上下文切换增加

### 13.3 如何解释轻量查询仍有抖动

即使启用了独立 UDF 池，轻量查询仍可能受到以下因素影响：

- 客户端连接池竞争
- 服务端全局共享资源竞争
- JVM 垃圾回收
- 底层存储或网络抖动

因此，关键不是 light query 完全不受影响，而是其退化幅度是否显著小于 `legacy`。

## 14. 有效性与威胁分析

### 14.1 内部有效性

潜在威胁包括：

- 压测线程本身实现存在偏差
- 失败样本未被正确过滤
- 不同实验点配置恢复不完整
- 会话池容量不足导致客户端成为瓶颈

缓解方式包括：

- 每个实验点重启 IGinX
- 记录 `status` 与 `error`
- 统一会话池配置
- 使用多轮重复取均值

### 14.2 构造有效性

本实验测量的是：

- IGinX 中 Python UDF 执行架构的系统级收益

而不是：

- Python 函数本身的微基准性能

因此结果应解释为“工程架构优化的端到端收益”，而不是“Python 解释器本身变快多少”。

### 14.3 外部有效性

本实验主要适用于：

- CPU 密集型 Python UDF
- 混合查询场景中存在明显 UDF 压力的系统负载

对以下场景仍需进一步实验：

- I/O 密集型 UDF
- 模型推理型 UDF
- 大规模 NumPy / pandas 主导型 UDF

## 15. 远程运行工作流

实验采用“本机编译，服务器只运行”的流程：

1. 本机编译 `core` 发行包。
2. 上传运行目录到远程服务器。
3. 服务器执行 `benchmark/run_udf_architecture_matrix.sh`。
4. 拉回以下结果：
   - `benchmark/results/architecture/raw-throughput.csv`
   - `benchmark/results/architecture/raw-latency-samples.csv`
   - `benchmark/results/architecture/logs/`
5. 本机执行 `test/benchmark/plot_udf_architecture_results.py`。

## 16. Smoke Test 建议

在正式实验前，建议先执行最小验证：

- `ARCHITECTURES="legacy adaptive"`
- `CONCURRENCIES="4"`
- `ROUNDS=1`
- `STEADY_INVOCATIONS_PER_THREAD=3`
- `MIXED_DURATION_SECONDS=10`
- `LIGHT_QUERY_CONCURRENCY=1`
- `LIGHT_QUERY_LIMIT=128`

Smoke test 目标是确认：

- 远程配置切换生效
- 两种架构都可正常运行
- 汇总 CSV 能写出
- 延迟样本 CSV 能写出
- 本机绘图脚本能顺利出图

## 17. 论文撰写建议

在论文中可按如下结构撰写该实验：

1. 实验目标
2. 对比架构说明
3. 实验环境
4. 变量设计
5. 指标定义
6. 稳态吞吐对比结果
7. UDF 尾延迟对比结果
8. 混合负载隔离结果
9. 结果分析与威胁说明

## 18. 结论模板

在尚未绑定具体数据前，可以预先准备如下结论模板：

“实验结果表明，相比于关闭 UDF 专用线程池的旧架构，启用独立自适应 UDF 线程池后，IGinX 在高并发 Python UDF 负载下表现出更高的稳态吞吐能力和更好的尾延迟控制效果。尤其在混合负载场景中，旧架构下轻量级非 UDF 查询会因 Python UDF 占用通用执行资源而出现明显的 P95 延迟恶化，而新架构通过线程池隔离显著减轻了这种干扰。这说明自适应 UDF 调度线程池不仅提升了 Python UDF 的并发执行能力，也改善了系统整体查询服务质量。”
