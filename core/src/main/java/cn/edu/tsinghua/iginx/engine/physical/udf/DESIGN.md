# IGinX 自适应 Python UDF 执行调度线程池 —— 工程设计文档

## 1 问题背景与动机

### 1.1 IGinX 中 Python UDF 的执行现状

IGinX 作为一个高性能多源异构数据库中间件，支持用户以 Python 编写用户自定义函数（UDF），包括 UDTF（行映射）、UDAF（集合映射）和 UDSF（普通映射）三种类型。Python UDF 通过 Pemja（一个 Java-Python 桥接库）在 JVM 进程内调用 CPython 解释器执行，避免了跨进程通信的开销。

在改造前的架构中，Python UDF 的执行依赖以下组件协作：

- **`MemoryPhysicalTaskDispatcher`**：单线程调度器，从任务队列中取出内存物理任务，提交到线程池执行。
- **`MemoryTaskThreadPoolExecutor`**：继承自 `AbstractTaskThreadPoolExecutor`，是一个**固定大小**的线程池（默认 200 线程），负责执行所有内存物理任务（包含但不限于 UDF 任务）。
- **`ThreadInterpreterManager`**：基于 `ThreadLocal<PythonInterpreter>` 管理每个线程上的 Python 解释器实例，实现"每线程一个解释器"的隔离模型。
- **`PyUDF`**：所有 Python UDF 的基类，其 `invokePyUDF()` 方法通过 `ThreadInterpreterManager` 在当前线程上同步执行 Python 代码。

执行链路如下：

```
SQL查询 → 物理计划 → MemoryPhysicalTaskDispatcher
  → MemoryTaskThreadPoolExecutor（固定200线程）
    → MemoryPhysicalTask.execute()
      → PyUDF.invokePyUDF()
        → ThreadInterpreterManager → Pemja PythonInterpreter
```

### 1.2 存在的问题

**问题一：固定线程池无法适应动态负载。** `MemoryTaskThreadPoolExecutor` 在启动时即固定为配置值（默认 200），运行期间不可调整。当 UDF 负载较轻时，大量空闲线程各自持有一个 PythonInterpreter 实例，造成内存浪费（每个 CPython 解释器实例约占 10-30 MB）；当 UDF 负载突增时，由于 UDF 任务与所有其他内存任务（投影、过滤、聚合等）共享同一个固定池，可能出现 UDF 任务排队等待而非 UDF 任务被阻塞的相互干扰。

**问题二：UDF 执行未与通用任务隔离。** 所有内存物理任务在同一个线程池中执行，缺乏针对 Python UDF 计算密集特性的独立资源管控。一个长时间运行的 Python UDF 会占据线程池中的一个槽位，影响其他查询的内存任务调度。

**问题三：Python 解释器资源泄漏。** 原有 `AbstractTaskThreadPoolExecutor` 的 `TaskThreadFactory` 使用 `Executors.defaultThreadFactory()` 创建线程，当线程因异常退出或被池回收时，其 `ThreadLocal` 上的 `PythonInterpreter` 未被显式关闭。`terminated()` 回调仅在整个线程池关闭时触发，无法覆盖单个线程退出的场景。

**问题四：与 Python 3.13 自由线程特性的契合。** 传统 CPython 的全局解释器锁（GIL）使得多线程下的 Python 执行实质上是串行的。Python 3.13 引入的自由线程（free-threading）模式取消了 GIL，允许真正的多线程并行执行 Python 代码。这要求线程池能够根据实际并行收益动态调整大小：在 GIL 存在时保守分配线程（避免无效竞争），在自由线程模式下积极扩展以充分利用多核。

### 1.3 设计目标

基于上述分析，本方案的设计目标为：

1. **构建独立的 UDF 专用线程池**，将 Python UDF 执行从通用内存任务线程池中剥离，实现资源隔离。
2. **实现池大小的动态自适应调节**，根据 CPU 利用率、任务队列长度和线程活跃度等指标，在运行时自动扩展或收缩线程池。
3. **保证 Python 解释器资源的正确生命周期管理**，线程退出时自动关闭其持有的 `PythonInterpreter`。
4. **提供完善的防振荡机制**，避免在负载波动时频繁扩缩导致的系统不稳定。
5. **最小化对现有代码的侵入**，通过单一集成点（`PyUDF.invokePyUDF()`）接入，不影响 `PyUDTF`、`PyUDAF`、`PyUDSF` 等子类的任何代码。

## 2 系统架构设计

### 2.1 整体架构

自适应 UDF 执行层由三个核心组件构成，以分层方式组织：

```
┌─────────────────────────────────────────────────────────┐
│                  AdaptiveUDFExecutor                      │
│                    （单例门面）                            │
│  ┌──────────────────────┐  ┌─────────────────────────┐  │
│  │ AdaptiveUDFThread-   │  │   AdaptiveScheduler     │  │
│  │ PoolExecutor         │←─│   （定时调度守护线程）    │  │
│  │ （可动态调整大小的    │  │                         │  │
│  │   UDF 专用线程池）    │  │  ┌───────────────────┐  │  │
│  │                      │  │  │SystemMetricsService│  │  │
│  │  ┌────────────────┐  │  │  │  （OSHI CPU/Mem）  │  │  │
│  │  │ UDFThreadFactory│  │  │  └───────────────────┘  │  │
│  │  │ （线程退出时     │  │  └─────────────────────────┘  │
│  │  │  清理interpreter）│  │                              │
│  │  └────────────────┘  │                                │
│  └──────────────────────┘                                │
└─────────────────────────────────────────────────────────┘
         ▲                              ▲
         │ submitAndGet(Callable)       │ 采集 CPU/内存指标
         │                              │
   PyUDF.invokePyUDF()          DefaultSystemMetricsService
```

**`AdaptiveUDFExecutor`（门面层）**：对外提供 `submitAndGet(Callable<T>)` 接口，封装线程池和调度器的完整生命周期。采用 Holder 单例模式，在首次调用 `getInstance()` 时初始化。当配置 `udfPoolEnabled=false` 时，`submitAndGet()` 直接在调用方线程上执行任务，退化为改造前的行为。

**`AdaptiveUDFThreadPoolExecutor`（执行层）**：继承 `java.util.concurrent.ThreadPoolExecutor`，专门用于执行 Python UDF 任务。与原有 `AbstractTaskThreadPoolExecutor` 的关键区别在于：支持运行时通过 `resizePool(int)` 动态调整 `corePoolSize` 和 `maximumPoolSize`；启用 `allowCoreThreadTimeOut(true)` 使空闲线程自动回收；自定义 `UDFThreadFactory` 在线程退出时通过 `finally` 块关闭 `ThreadLocal` 上的 `PythonInterpreter`。

**`AdaptiveScheduler`（决策层）**：单线程定时守护进程，周期性采集系统指标（CPU 利用率、任务队列长度、活跃线程数），通过 EWMA 平滑后与阈值比较，做出扩展或收缩决策。内置冷却期和连续空闲计数机制，防止调节振荡。

### 2.2 与现有系统的集成

集成点仅有一个：`PyUDF.invokePyUDF()` 方法。

**改造前：**

```java
protected List<List<Object>> invokePyUDF(
    List<List<Object>> data, List<Object> args, Map<String, Object> kvargs) {
  long timeout = config.getUDFTimeout();
  String obj = (moduleName + className).replace(".", "a");
  ThreadInterpreterManager.exec(
      String.format("import %s; %s = %s.%s()", moduleName, obj, moduleName, className));
  return ThreadInterpreterManager.invokeMethodWithTimeout(
      timeout, obj, UDF_FUNC, data, args, kvargs);
}
```

Python 代码直接在调用方的内存任务线程上执行。

**改造后：**

```java
protected List<List<Object>> invokePyUDF(
    List<List<Object>> data, List<Object> args, Map<String, Object> kvargs) {
  try {
    return AdaptiveUDFExecutor.getInstance()
        .submitAndGet(() -> {
          long timeout = config.getUDFTimeout();
          String obj = (moduleName + className).replace(".", "a");
          ThreadInterpreterManager.exec(
              String.format("import %s; %s = %s.%s()", moduleName, obj, moduleName, className));
          return ThreadInterpreterManager.invokeMethodWithTimeout(
              timeout, obj, UDF_FUNC, data, args, kvargs);
        });
  } catch (RuntimeException e) {
    throw e;
  } catch (Exception e) {
    throw new RuntimeException("Failed to execute Python UDF: " + moduleName, e);
  }
}
```

Python 代码被包装为 `Callable`，提交到自适应 UDF 线程池执行。内存任务线程通过 `Future.get()` 阻塞等待结果。此改造对所有 `PyUDF` 子类（`PyUDTF`、`PyUDAF`、`PyUDSF`）完全透明。

改造后的完整执行链路：

```
SQL查询 → 物理计划 → MemoryPhysicalTaskDispatcher
  → MemoryTaskThreadPoolExecutor（固定200线程，仅协调）
    → MemoryPhysicalTask.execute()
      → PyUDF.invokePyUDF()
        → AdaptiveUDFExecutor.submitAndGet(Callable)
          → AdaptiveUDFThreadPoolExecutor（动态大小）
            → ThreadInterpreterManager → Pemja PythonInterpreter
          ← Future.get() 返回结果
```

## 3 核心模块详细设计

### 3.1 AdaptiveUDFThreadPoolExecutor —— 可动态调整的 UDF 线程池

**类继承关系：** 直接继承 `java.util.concurrent.ThreadPoolExecutor`（而非 `AbstractTaskThreadPoolExecutor`），独立管理自身的线程工厂和解释器配置，避免与通用任务线程池的耦合。

**构造参数：**

|         参数         |                          含义                           |
|--------------------|-------------------------------------------------------|
| `initialSize`      | 线程池初始大小，即启动时的 `corePoolSize`                          |
| `minPoolSize`      | 动态调节的下界                                               |
| `maxPoolSize`      | 动态调节的上界，同时作为 `ThreadPoolExecutor` 的 `maximumPoolSize` |
| `keepAliveSeconds` | 空闲线程存活时间，配合 `allowCoreThreadTimeOut(true)` 实现自然收缩     |

**`resizePool(int newSize)` 方法的设计：**

```java
public void resizePool(int newSize) {
    newSize = Math.max(newSize, minPoolSize);   // 下界夹紧
    newSize = Math.min(newSize, maxPoolSize);   // 上界夹紧
    int oldSize = getCorePoolSize();
    if (newSize == oldSize) return;             // 无变化则跳过
    if (newSize > oldSize) {
        setMaximumPoolSize(newSize);            // 扩展：先提升上限再扩核心
        setCorePoolSize(newSize);
    } else {
        setCorePoolSize(newSize);               // 收缩：先降核心再降上限
        setMaximumPoolSize(newSize);
    }
}
```

扩展和收缩时调用 `setMaximumPoolSize` 和 `setCorePoolSize` 的**顺序不同**，这是因为 `ThreadPoolExecutor` 要求 `corePoolSize <= maximumPoolSize` 始终成立。扩展时先提升上限以容纳新的核心数，收缩时先降低核心数以满足更小的上限约束。这两个方法均为 `ThreadPoolExecutor` 内置的线程安全操作。

**`UDFThreadFactory` 的设计：**

```java
private static class UDFThreadFactory implements ThreadFactory {
    @Override
    public Thread newThread(Runnable r) {
        Thread thread = new Thread(() -> {
            try {
                r.run();
            } finally {
                if (ThreadInterpreterManager.isInterpreterSet()) {
                    ThreadInterpreterManager.getInterpreter().close();
                }
            }
        }, namePrefix + threadNumber.getAndIncrement());
        thread.setDaemon(true);
        return thread;
    }
}
```

关键设计：将 `ThreadPoolExecutor` 内部 Worker 的 `run()` 方法包装在 `try-finally` 中。Worker 的 `run()` 是一个循环，不断从队列中取任务执行；当线程因空闲超时或池收缩而退出循环时，`finally` 块确保该线程的 `ThreadLocal<PythonInterpreter>` 被正确关闭释放。这解决了原有架构中线程退出时解释器泄漏的问题。

**`beforeExecute` 钩子：**

```java
@Override
protected void beforeExecute(Thread t, Runnable r) {
    super.beforeExecute(t, r);
    ThreadInterpreterManager.setConfig(interpreterConfig);
}
```

在每个任务执行前，将 `PythonInterpreterConfig` 设置到当前线程的 `ThreadLocal` 中。`ThreadInterpreterManager.getInterpreter()` 在首次调用时会基于此配置懒创建 `PythonInterpreter` 实例。同一线程后续的 UDF 任务复用同一个解释器，避免重复创建的开销。

### 3.2 AdaptiveScheduler —— 自适应调度决策引擎

#### 3.2.1 核心调度循环

`AdaptiveScheduler` 基于 `ScheduledExecutorService` 实现非固定间隔的周期调度。每次调度回调执行以下步骤：

```
┌───────────────────┐
│   采集原始指标     │  rawCpu, rawQueueSize, activeCount, currentSize
└────────┬──────────┘
         ▼
┌───────────────────┐
│   EWMA 平滑处理   │  ewmaCpu = α·raw + (1-α)·prev
└────────┬──────────┘
         ▼
┌───────────────────┐
│   冷却期检查       │  若在冷却窗口内 → 跳过决策，直接返回
└────────┬──────────┘
         ▼
┌───────────────────┐
│   计算活跃线程占比 │  activeRatio = activeCount / currentSize
└────────┬──────────┘
         ▼
┌──────────┴──────────┐
│    扩展判定          │    收缩判定          │    稳态
│  ewmaQueue > Texp   │  ewmaQueue ≤ Tshr   │  重置空闲计数
│  AND cpu < cpuHigh  │  AND cpu < cpuLow   │  恢复基准间隔
│  AND ratio ≥ 0.8    │  AND ratio < 0.3    │
│  AND size < max     │  AND size > min     │
└────────┬────────────┘────────┬─────────────┘
         ▼                     ▼
   newSize = min(cur*2, max)  consecutiveIdle++
   resizePool(newSize)        if idle ≥ 3:
   markAdjusted()               newSize = max(cur/2, min)
   加速调度间隔                  resizePool(newSize)
                                减速调度间隔
```

#### 3.2.2 EWMA 指标平滑

系统指标（CPU 利用率、队列长度）在短时间内可能存在剧烈波动。例如，一批 UDF 任务同时提交会导致队列瞬间增长，但很快被线程消费后回落。若直接基于瞬时值做决策，容易导致不必要的扩展。

本方案采用**指数加权移动平均（Exponentially Weighted Moving Average, EWMA）**对指标进行平滑处理：

$$
S_t = \alpha \cdot X_t + (1 - \alpha) \cdot S_{t-1}
$$

其中 $X_t$ 为当前周期的原始采样值，$S_{t-1}$ 为上一周期的 EWMA 值，$\alpha$ 为平滑系数。本方案取 $\alpha = 0.3$，意味着当前值占 30% 权重，历史值占 70% 权重。该系数使得 EWMA 对持续性趋势具有足够的跟踪能力，同时有效过滤单次采样的噪声。

EWMA 的一个重要特性是即使在冷却期内（不做扩缩决策），指标仍然持续更新。这保证了冷却期结束后，调度器掌握的是最新的平滑指标，而非过时的历史数据。

#### 3.2.3 扩展策略

扩展需同时满足以下四个条件：

1. **队列积压超过阈值**：`ewmaQueueSize > expandThreshold`，表明有持续的任务排队等待。
2. **CPU 未过载**：`ewmaCpuUsage < cpuHighThreshold`，确保系统仍有计算余量。若 CPU 已接近饱和，扩展线程数只会加剧竞争而无法提升吞吐。
3. **活跃线程占比高**：`activeRatio >= 0.8`，即现有线程中至少 80% 正在执行任务。此条件排除了"队列短暂堆积但线程其实不忙"的情况（例如任务刚入队尚未被分配）。
4. **未达上限**：`currentSize < maxPoolSize`。

扩展采用**倍增策略**：`newSize = min(currentSize * 2, maxPoolSize)`。倍增（而非线性增长）能够在负载快速上升时迅速响应，同时上限约束保证不会无限制扩展。

#### 3.2.4 收缩策略

收缩策略相较于扩展更为保守，设计了两层防护：

**第一层——连续空闲计数：** 收缩条件（队列低于阈值、CPU 低于下限、活跃线程占比低于 30%）必须**连续满足 3 个调度周期**以上才执行收缩。这避免了间歇性负载（例如每隔几秒来一批 UDF 请求）导致的反复收缩-扩展。

**第二层——渐进式缩减：** `newSize = max(currentSize / 2, minPoolSize)`。每次最多将池大小减半，而非直接缩至最小值。这保证了如果负载再次上升，仍有一定的缓冲线程可用，无需从最小值重新扩展。

收缩后，被标记为多余的核心线程不会被强制中断。由于启用了 `allowCoreThreadTimeOut(true)`，这些线程会在 `keepAliveSeconds`（默认 30 秒）后因空闲超时自动退出，`UDFThreadFactory` 的 `finally` 块保证其 `PythonInterpreter` 被正确关闭。

#### 3.2.5 冷却期与自适应调度间隔

**冷却期（Cooldown）**：每次执行扩展或收缩操作后，记录时间戳 `lastAdjustTime`。在 `cooldownMs`（默认 5 秒）窗口内，即使指标满足调节条件也不执行操作。冷却期的目的是等待上一次调节的效果充分体现——新线程需要时间创建和执行任务，收缩的线程需要时间完成当前任务并退出。过早的二次调节可能基于尚未稳定的状态做出错误决策。

**自适应调度间隔**：调度器不使用固定的 `scheduleAtFixedRate`，而是在每次回调结束后通过 `scheduler.schedule()` 动态设置下一次回调的延迟：

|         状态          |                      调度间隔                      |
|---------------------|------------------------------------------------|
| 执行了扩展操作             | `max(baseInterval / 2, 500ms)` — 加速检测以快速跟踪负载上升 |
| 满足收缩条件（但尚未达到连续空闲阈值） | `min(baseInterval * 2, 10000ms)` — 减速检测以降低调度开销 |
| 稳态（既不扩展也不收缩）        | `baseInterval` — 恢复基准间隔                        |

### 3.3 AdaptiveUDFExecutor —— 单例门面

#### 3.3.1 生命周期管理

`AdaptiveUDFExecutor` 采用静态内部类 Holder 实现线程安全的懒加载单例：

```java
private static class Holder {
    private static final AdaptiveUDFExecutor INSTANCE = new AdaptiveUDFExecutor();
}
```

构造时按以下顺序初始化：
1. 读取 `Config` 中的全部自适应线程池配置参数。
2. 若 `udfPoolEnabled == true`：创建 `DefaultSystemMetricsService` 并启动 CPU/内存采集；创建 `AdaptiveUDFThreadPoolExecutor`；创建 `AdaptiveScheduler` 并启动定时调度。
3. 若 `udfPoolEnabled == false`：所有组件设为 `null`，`submitAndGet()` 退化为直接在调用方线程执行。

#### 3.3.2 异常传播

`submitAndGet()` 的异常处理遵循透明传播原则：

```java
public <T> T submitAndGet(Callable<T> task) throws Exception {
    Future<T> future = pool.submit(task);
    try {
        return future.get();
    } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof Exception) {
            throw (Exception) cause;   // 解包并抛出原始异常
        }
        throw new RuntimeException(cause);
    }
}
```

`Future.get()` 将任务中的异常包装为 `ExecutionException`，此处对其解包后重新抛出原始异常。这保证了调用方（`PyUDF` 子类的 `transform()` 方法）看到的异常与改造前完全一致，不会引入额外的异常层级。

## 4 解释器生命周期管理

### 4.1 原有问题分析

改造前的 `AbstractTaskThreadPoolExecutor.TaskThreadFactory` 使用 `Executors.defaultThreadFactory()` 创建线程：

```java
// 改造前
Thread thread = defaultFactory.newThread(r);
thread.setUncaughtExceptionHandler(...);
return thread;
```

当线程池缩容或线程因异常退出时，线程的 `ThreadLocal<PythonInterpreter>` 无处清理。`terminated()` 回调仅在整个池执行 `shutdown()` 后所有线程结束时触发一次，且仅在调用 `terminated()` 的那个线程上检查 `ThreadLocal`，无法覆盖已退出的其他线程。

### 4.2 改造方案

同时在两处实施修复：

**`AdaptiveUDFThreadPoolExecutor.UDFThreadFactory`**（新增类）和 **`AbstractTaskThreadPoolExecutor.TaskThreadFactory`**（修改已有类），均采用相同的模式：

```java
Thread thread = new Thread(() -> {
    try {
        r.run();           // ThreadPoolExecutor Worker 的主循环
    } finally {
        if (ThreadInterpreterManager.isInterpreterSet()) {
            ThreadInterpreterManager.getInterpreter().close();
        }
    }
}, threadName);
```

`ThreadPoolExecutor` 的内部 Worker 对象实现了 `Runnable`，其 `run()` 方法是一个循环体，反复从 `BlockingQueue` 中获取任务并执行。当以下任一情况发生时，`run()` 返回：
- 线程因 `keepAliveTime` 超时未获取到新任务。
- `setCorePoolSize()` 缩小了核心线程数，多余的空闲线程被中断后退出循环。
- 线程池被 `shutdown()`。

无论哪种退出路径，`finally` 块都会执行，确保 `PythonInterpreter.close()` 被调用，释放 CPython 解释器持有的全部资源（内存、文件句柄、GIL 等）。

## 5 配置参数设计

所有配置项遵循 IGinX 现有的配置加载模式：`config.properties` 文件定义 → `ConfigDescriptor` 解析 → `Config` 对象存储 → 支持环境变量覆盖。

|             配置项             |   类型    |    默认值    |                                  说明                                   |
|-----------------------------|---------|-----------|-----------------------------------------------------------------------|
| `udfPoolEnabled`            | boolean | `true`    | 总开关。`false` 时 UDF 在调用方线程直接执行（与改造前行为一致），用于对比测试和回退                      |
| `udfPoolMinThreads`         | int     | `2`       | 线程池最小线程数，池收缩的下界                                                       |
| `udfPoolMaxThreads`         | int     | CPU核数 × 2 | 线程池最大线程数，池扩展的上界。默认值取 `Runtime.getRuntime().availableProcessors() * 2` |
| `udfPoolInitialThreads`     | int     | `4`       | 线程池启动时的初始大小                                                           |
| `udfPoolKeepAliveSeconds`   | long    | `30`      | 空闲线程存活时间（秒）。超过此时间未执行任务的线程自动退出                                         |
| `udfPoolExpandThreshold`    | int     | `5`       | 扩展阈值：EWMA 平滑后的队列长度超过此值时考虑扩展                                           |
| `udfPoolShrinkThreshold`    | int     | `1`       | 收缩阈值：EWMA 平滑后的队列长度低于此值时考虑收缩                                           |
| `udfPoolCpuHighThreshold`   | double  | `0.85`    | CPU 高水位。CPU 利用率超过此值时禁止扩展，防止系统过载                                       |
| `udfPoolCpuLowThreshold`    | double  | `0.3`     | CPU 低水位。CPU 利用率低于此值且队列空闲时触发收缩评估                                       |
| `udfPoolScheduleIntervalMs` | long    | `2000`    | 调度器基准检查间隔（毫秒）。实际间隔会根据系统状态自适应调整                                        |
| `udfPoolCooldownMs`         | long    | `5000`    | 冷却时间（毫秒）。每次扩缩后在此时间窗口内不做进一步调节                                          |

## 6 文件变更清单

### 6.1 新增文件

|                            文件                            |                   职责                    |
|----------------------------------------------------------|-----------------------------------------|
| `engine/physical/udf/AdaptiveUDFThreadPoolExecutor.java` | 可动态调整大小的 UDF 专用线程池，含 `UDFThreadFactory` |
| `engine/physical/udf/AdaptiveScheduler.java`             | EWMA + 冷却期 + 自适应间隔的调度决策引擎               |
| `engine/physical/udf/AdaptiveUDFExecutor.java`           | 单例门面，管理池和调度器生命周期                        |

### 6.2 修改文件

|                          文件                           |                                   变更内容                                    |
|-------------------------------------------------------|---------------------------------------------------------------------------|
| `engine/shared/function/udf/python/PyUDF.java`        | `invokePyUDF()` 改为通过 `AdaptiveUDFExecutor.submitAndGet()` 提交（仅修改方法体，接口不变） |
| `engine/physical/AbstractTaskThreadPoolExecutor.java` | `TaskThreadFactory` 增加 `try-finally` 包装，线程退出时自动 close interpreter         |
| `conf/Config.java`                                    | 新增 11 个 UDF 线程池配置字段及 getter/setter                                        |
| `conf/ConfigDescriptor.java`                          | 在 `loadPropsFromFile` 和 `loadPropsFromEnv` 中添加新配置项的解析逻辑                   |
| `conf/config.properties`                              | 添加带中文注释的默认配置段                                                             |

## 7 测试设计

### 7.1 测试策略

测试类 `AdaptiveUDFThreadPoolTest` 包含 14 个测试用例，覆盖以下维度：

**线程池基础功能（7 个用例）：**
- 初始大小验证
- 扩展 / 收缩 / 边界夹紧（低于 min、超过 max）/ 相同大小 no-op
- 任务执行正确性
- 并发任务执行
- 运行中动态 resize

**调度器决策逻辑（5 个用例）：**
- 扩展触发（EWMA 多轮收敛后超过阈值）
- 收缩需连续 3 轮空闲
- 冷却期内不做调节
- CPU 过载时禁止扩展
- EWMA 平滑效果验证

### 7.2 测试隔离设计

测试通过两个机制实现与 Python 环境的解耦：

1. **`AdaptiveUDFThreadPoolExecutor` 的双参数构造器**：接受显式的 `PythonInterpreterConfig` 参数，测试中传入 dummy config，避免触发 `FunctionManager` 的初始化。
2. **`StubMetrics` 桩对象**：实现 `SystemMetricsService` 接口，允许测试精确控制 CPU 和内存指标的返回值，模拟各种负载场景。

```java
private static class StubMetrics implements SystemMetricsService {
    private volatile double cpu;
    void setCpu(double cpu) { this.cpu = cpu; }
    @Override public double getRecentCpuUsage() { return cpu; }
    // ...
}
```

## 8 设计决策与权衡

### 8.1 为何不复用 AbstractTaskThreadPoolExecutor

`AbstractTaskThreadPoolExecutor` 设计为固定大小池，其构造器将 `corePoolSize` 和 `maximumPoolSize` 设为相同值且 `keepAliveTime = 0`。虽然可以通过子类覆盖来修改这些参数，但 `AbstractTaskThreadPoolExecutor` 还承担着为 `MemoryTaskThreadPoolExecutor` 和 `StorageTaskThreadPoolExecutor` 服务的职责。修改其行为可能影响存储任务和通用内存任务的执行语义。因此 `AdaptiveUDFThreadPoolExecutor` 选择直接继承 `ThreadPoolExecutor`，获得完全的参数控制权。

### 8.2 为何使用 setCorePoolSize 而非手动管理线程

`ThreadPoolExecutor.setCorePoolSize()` 是 JDK 提供的线程安全方法，内部通过 `mainLock` 保证并发正确性。当核心数增加时，如果队列中有等待任务，会自动创建新线程；当核心数减少时，多余的空闲线程在下次尝试获取任务时会被优雅回收。相比手动创建/销毁线程，此方案更简洁可靠，且避免了自行实现线程安全控制的复杂度。

### 8.3 为何 EWMA 在冷却期内仍然更新

```java
// EWMA 更新在冷却期检查之前
ewmaCpuUsage = EWMA_ALPHA * rawCpu + (1 - EWMA_ALPHA) * ewmaCpuUsage;
ewmaQueueSize = EWMA_ALPHA * rawQueueSize + (1 - EWMA_ALPHA) * ewmaQueueSize;

if (inCooldown()) {
    return;  // 跳过决策，但 EWMA 已更新
}
```

如果冷却期内暂停 EWMA 更新，那么冷却期结束后的第一次决策将基于过时的数据。假设冷却期为 5 秒、调度间隔为 2 秒，则冷却期内有 2-3 次采样被丢弃。在冷却期结束后，EWMA 从旧值突变到新值，可能导致错误的扩缩决策。持续更新 EWMA 保证了冷却期结束时调度器拥有平滑的、反映最新趋势的指标。

### 8.4 为何收缩需要连续 3 轮空闲

许多 UDF 工作负载呈间歇性特征：一批查询到达后集中执行 UDF，完成后进入短暂空闲，随后下一批查询到来。如果仅基于单次空闲判断就触发收缩，可能导致频繁的收缩-扩展循环（thrashing）。每次扩展都需要创建新线程和初始化 `PythonInterpreter`（约 100-500ms），频繁收缩-扩展会显著增加延迟。

连续 3 轮空闲的要求（在基准间隔 2 秒下约为 6 秒持续空闲）为间歇性负载提供了缓冲窗口。结合冷却期机制，完整的收缩防护链为：`3 × 调度间隔 + 冷却期 = 约 11 秒`的空闲确认时间。
