/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.tools.benchmark;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.pool.SessionPool;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Independent benchmark runner for comparing legacy and adaptive Python UDF execution
 * architectures. It keeps the free-threading benchmark untouched while extending the benchmark
 * dimensions to include latency percentiles and mixed-load isolation.
 */
public class AdaptiveUdfArchitectureBenchmarkRunner {

  private static final int DEFAULT_TIMEOUT_SECONDS = 30;
  private static final String SERIES_PREFIX = "bench.arch";
  private static final String COMPUTE_FUNCTION_NAME = "arch_latency_udf_bench";
  private static final String META_FUNCTION_NAME = "arch_meta_bench";
  private static final String UDF_SCRIPT_NAME = "latency_benchmark_udsf.py";
  private static final String REGISTER_SQL_TEMPLATE =
      "CREATE FUNCTION UDSF \"%s\" FROM \"%s\" IN \"%s\";";
  private static final String DROP_SQL_TEMPLATE = "DROP FUNCTION \"%s\";";
  private static final String THROUGHPUT_CSV_HEADER =
      "architecture,round,scenario,concurrency,rows,cols,loopsPerInvocation,"
          + "initialThreads,minThreads,maxThreads,steadyInvocationsPerThread,"
          + "mixedDurationSeconds,lightQueryConcurrency,lightQueryLimit,totalInvocations,"
          + "elapsedMs,throughput,pythonVersion,gilEnabled,status,error";
  private static final String LATENCY_CSV_HEADER =
      "architecture,round,scenario,workloadType,concurrency,requestId,latencyMs";
  private static final String SCENARIO_STEADY = "steady_udf";
  private static final String SCENARIO_MIXED = "mixed_projection";
  private static final String WORKLOAD_UDF = "udf";
  private static final String WORKLOAD_LIGHT = "light";

  public static void main(String[] args) throws Exception {
    BenchmarkConfig config = BenchmarkConfig.fromArgs(args);
    new AdaptiveUdfArchitectureBenchmarkRunner().run(config);
  }

  private void run(BenchmarkConfig config) throws Exception {
    Session controlSession =
        new Session(config.host, config.port, config.username, config.password);
    List<BenchmarkRecord> records = new ArrayList<>();
    List<LatencySample> latencySamples = new ArrayList<>();
    boolean sessionOpened = false;
    try {
      controlSession.openSession();
      sessionOpened = true;
      registerFunctions(controlSession, config);
      insertData(controlSession, config.rows, config.cols);
      PythonModeInfo pythonMode = fetchPythonMode(controlSession, config);

      try {
        warmUp(config);
        ScenarioResult steadyResult = runSteadyScenario(config, pythonMode);
        records.add(steadyResult.record);
        latencySamples.addAll(steadyResult.latencySamples);
      } catch (Exception e) {
        records.add(BenchmarkRecord.failure(config, pythonMode, SCENARIO_STEADY, e));
      }

      try {
        warmUp(config);
        ScenarioResult mixedResult = runMixedProjectionScenario(config, pythonMode);
        records.add(mixedResult.record);
        latencySamples.addAll(mixedResult.latencySamples);
      } catch (Exception e) {
        records.add(BenchmarkRecord.failure(config, pythonMode, SCENARIO_MIXED, e));
      }
    } finally {
      if (sessionOpened) {
        cleanup(controlSession);
      }
      appendThroughputCsv(config.throughputCsv, records);
      appendLatencyCsv(config.latencyCsv, latencySamples);
    }
  }

  private void registerFunctions(Session session, BenchmarkConfig config) throws SessionException {
    safeExecuteSql(session, String.format(DROP_SQL_TEMPLATE, COMPUTE_FUNCTION_NAME));
    safeExecuteSql(session, String.format(DROP_SQL_TEMPLATE, META_FUNCTION_NAME));
    safeExecuteSql(session, "clear data;");

    executeSqlAndCheck(
        session,
        String.format(
            REGISTER_SQL_TEMPLATE,
            META_FUNCTION_NAME,
            "ArchitectureBenchmarkMetadata",
            config.udfScriptPath));
    executeSqlAndCheck(
        session,
        String.format(
            REGISTER_SQL_TEMPLATE,
            COMPUTE_FUNCTION_NAME,
            "ArchitectureComputeIntensiveBenchmark",
            config.udfScriptPath));
  }

  private void insertData(Session session, int rows, int cols) throws SessionException {
    List<String> paths = new ArrayList<>();
    List<DataType> dataTypes = new ArrayList<>();
    for (int i = 1; i <= cols; i++) {
      paths.add(SERIES_PREFIX + ".s" + i);
      dataTypes.add(DataType.DOUBLE);
    }

    long[] keys = new long[rows];
    Object[] valuesList = new Object[rows];
    for (int row = 0; row < rows; row++) {
      keys[row] = row;
      Object[] rowValues = new Object[cols];
      for (int col = 0; col < cols; col++) {
        rowValues[col] = row + col / 10.0d;
      }
      valuesList[row] = rowValues;
    }
    session.insertRowRecords(paths, keys, valuesList, dataTypes, null);
  }

  private PythonModeInfo fetchPythonMode(Session session, BenchmarkConfig config)
      throws SessionException {
    if (config.pythonExec != null && !config.pythonExec.trim().isEmpty()) {
      try {
        return probePythonMode(config.pythonExec);
      } catch (Exception ignored) {
        // Fall back to server-side metadata query.
      }
    }
    SessionExecuteSqlResult result =
        executeSqlAndCheck(
            session,
            String.format(
                "SELECT %s(*) FROM %s WHERE key < 1;", META_FUNCTION_NAME, SERIES_PREFIX));
    List<List<Object>> values = result.getValues();
    if (values == null || values.isEmpty()) {
      throw new IllegalStateException("metadata query returned no rows");
    }
    List<Object> row = values.get(0);
    return new PythonModeInfo(
        String.valueOf(((Number) row.get(0)).intValue()),
        ((Number) row.get(1)).intValue()
            + "."
            + ((Number) row.get(2)).intValue()
            + "."
            + ((Number) row.get(3)).intValue());
  }

  private PythonModeInfo probePythonMode(String pythonExec) throws Exception {
    Process process =
        new ProcessBuilder(
                pythonExec,
                "-c",
                "import sys; "
                    + "v=sys.version_info; "
                    + "print(int(getattr(sys, '_is_gil_enabled', lambda: True)())); "
                    + "print(f'{v.major}.{v.minor}.{v.micro}')")
            .start();
    String stdout = readStream(process.getInputStream());
    String stderr = readStream(process.getErrorStream());
    int exitCode = process.waitFor();
    if (exitCode != 0) {
      throw new IllegalStateException(
          "failed to probe python mode from "
              + pythonExec
              + ", exitCode="
              + exitCode
              + ", stderr="
              + stderr);
    }
    String[] lines = stdout.split("\\R");
    if (lines.length < 2) {
      throw new IllegalStateException("unexpected python probe output: " + stdout);
    }
    return new PythonModeInfo(lines[0].trim(), lines[1].trim());
  }

  private String readStream(InputStream stream) throws IOException {
    try (InputStream input = stream;
        ByteArrayOutputStream output = new ByteArrayOutputStream()) {
      byte[] buffer = new byte[1024];
      int read;
      while ((read = input.read(buffer)) != -1) {
        output.write(buffer, 0, read);
      }
      return output.toString(StandardCharsets.UTF_8.name()).trim();
    }
  }

  private void warmUp(BenchmarkConfig config) throws Exception {
    SessionPool pool =
        new SessionPool(
            config.host,
            config.port,
            config.username,
            config.password,
            Math.max(config.concurrency + config.lightQueryConcurrency, 4));
    String udfStatement =
        buildUdfStatement(
            Math.min(config.rows, 128), Math.max(1, Math.min(config.loopsPerInvocation, 2)));
    String lightStatement = buildLightStatement(Math.min(config.lightQueryLimit, 128));
    try {
      for (int i = 0; i < Math.max(1, config.warmupRounds); i++) {
        executeSqlAndCheck(pool, udfStatement);
        executeSqlAndCheck(pool, lightStatement);
      }
    } finally {
      pool.close();
    }
  }

  private ScenarioResult runSteadyScenario(BenchmarkConfig config, PythonModeInfo pythonMode)
      throws Exception {
    String statement = buildUdfStatement(config.rows, config.loopsPerInvocation);
    SessionPool pool =
        new SessionPool(
            config.host,
            config.port,
            config.username,
            config.password,
            Math.max(config.concurrency * 2, 4));
    ExecutorService callers = Executors.newFixedThreadPool(config.concurrency);
    CountDownLatch ready = new CountDownLatch(config.concurrency);
    CountDownLatch start = new CountDownLatch(1);
    AtomicLong requestIdGenerator = new AtomicLong(0L);
    List<Future<TaskMetrics>> futures = new ArrayList<>();

    try {
      for (int i = 0; i < config.concurrency; i++) {
        futures.add(
            callers.submit(
                createFixedInvocationTask(
                    pool,
                    statement,
                    config.steadyInvocationsPerThread,
                    ready,
                    start,
                    config.timeoutSeconds,
                    requestIdGenerator,
                    config,
                    SCENARIO_STEADY,
                    WORKLOAD_UDF)));
      }
      awaitReady(ready, config.timeoutSeconds, "steady benchmark callers");
      long beginNs = System.nanoTime();
      start.countDown();
      List<TaskMetrics> taskMetrics = collectTaskMetrics(futures);
      long elapsedNs = System.nanoTime() - beginNs;
      return buildScenarioResult(
          config,
          pythonMode,
          SCENARIO_STEADY,
          elapsedNs,
          taskMetrics,
          new ArrayList<LatencySample>());
    } finally {
      shutdownExecutor(callers, config.timeoutSeconds);
      pool.close();
    }
  }

  private ScenarioResult runMixedProjectionScenario(
      BenchmarkConfig config, PythonModeInfo pythonMode) throws Exception {
    int totalCallers = config.concurrency + config.lightQueryConcurrency;
    SessionPool pool =
        new SessionPool(
            config.host,
            config.port,
            config.username,
            config.password,
            Math.max(totalCallers * 2, 4));
    ExecutorService callers = Executors.newFixedThreadPool(totalCallers);
    CountDownLatch ready = new CountDownLatch(totalCallers);
    CountDownLatch start = new CountDownLatch(1);
    AtomicLong requestIdGenerator = new AtomicLong(0L);
    List<Future<TaskMetrics>> udfFutures = new ArrayList<>();
    List<Future<TaskMetrics>> lightFutures = new ArrayList<>();
    String udfStatement = buildUdfStatement(config.rows, config.loopsPerInvocation);
    String lightStatement = buildLightStatement(config.lightQueryLimit);
    long durationNs = TimeUnit.SECONDS.toNanos(config.mixedDurationSeconds);

    try {
      for (int i = 0; i < config.concurrency; i++) {
        udfFutures.add(
            callers.submit(
                createTimedTask(
                    pool,
                    udfStatement,
                    ready,
                    start,
                    config.timeoutSeconds,
                    durationNs,
                    requestIdGenerator,
                    config,
                    SCENARIO_MIXED,
                    WORKLOAD_UDF)));
      }
      for (int i = 0; i < config.lightQueryConcurrency; i++) {
        lightFutures.add(
            callers.submit(
                createTimedTask(
                    pool,
                    lightStatement,
                    ready,
                    start,
                    config.timeoutSeconds,
                    durationNs,
                    requestIdGenerator,
                    config,
                    SCENARIO_MIXED,
                    WORKLOAD_LIGHT)));
      }
      awaitReady(ready, config.timeoutSeconds, "mixed benchmark callers");
      long beginNs = System.nanoTime();
      start.countDown();
      List<TaskMetrics> udfTaskMetrics = collectTaskMetrics(udfFutures);
      List<TaskMetrics> lightTaskMetrics = collectTaskMetrics(lightFutures);
      long elapsedNs = System.nanoTime() - beginNs;
      return buildScenarioResult(
          config,
          pythonMode,
          SCENARIO_MIXED,
          elapsedNs,
          udfTaskMetrics,
          flattenSamples(lightTaskMetrics));
    } finally {
      shutdownExecutor(callers, config.timeoutSeconds);
      pool.close();
    }
  }

  private ScenarioResult buildScenarioResult(
      BenchmarkConfig config,
      PythonModeInfo pythonMode,
      String scenario,
      long elapsedNs,
      List<TaskMetrics> udfTaskMetrics,
      List<LatencySample> extraSamples) {
    List<LatencySample> samples = new ArrayList<>();
    long totalInvocations = 0L;
    for (TaskMetrics metrics : udfTaskMetrics) {
      totalInvocations += metrics.completedInvocations;
      samples.addAll(metrics.samples);
    }
    samples.addAll(extraSamples);

    double elapsedMs = elapsedNs / 1_000_000.0d;
    double throughput = totalInvocations <= 0 ? 0.0d : totalInvocations * 1_000.0d / elapsedMs;
    BenchmarkRecord record =
        BenchmarkRecord.success(
            config, pythonMode, scenario, totalInvocations, elapsedMs, throughput);
    return new ScenarioResult(record, samples);
  }

  private List<LatencySample> flattenSamples(List<TaskMetrics> taskMetrics) {
    List<LatencySample> samples = new ArrayList<>();
    for (TaskMetrics metrics : taskMetrics) {
      samples.addAll(metrics.samples);
    }
    return samples;
  }

  private Callable<TaskMetrics> createFixedInvocationTask(
      final SessionPool pool,
      final String statement,
      final int invocations,
      final CountDownLatch ready,
      final CountDownLatch start,
      final int timeoutSeconds,
      final AtomicLong requestIdGenerator,
      final BenchmarkConfig config,
      final String scenario,
      final String workloadType) {
    return () -> {
      ready.countDown();
      if (!start.await(timeoutSeconds, TimeUnit.SECONDS)) {
        throw new IllegalStateException("benchmark start signal timed out");
      }

      List<LatencySample> localSamples = new ArrayList<>();
      for (int i = 0; i < invocations; i++) {
        if (Thread.currentThread().isInterrupted()) {
          throw new InterruptedException("benchmark caller interrupted");
        }
        long beginNs = System.nanoTime();
        executeSqlAndCheck(pool, statement);
        long requestId = requestIdGenerator.incrementAndGet();
        double latencyMs = (System.nanoTime() - beginNs) / 1_000_000.0d;
        localSamples.add(
            new LatencySample(
                config.architecture,
                config.round,
                scenario,
                workloadType,
                config.concurrency,
                requestId,
                latencyMs));
      }
      return new TaskMetrics(invocations, localSamples);
    };
  }

  private Callable<TaskMetrics> createTimedTask(
      final SessionPool pool,
      final String statement,
      final CountDownLatch ready,
      final CountDownLatch start,
      final int timeoutSeconds,
      final long durationNs,
      final AtomicLong requestIdGenerator,
      final BenchmarkConfig config,
      final String scenario,
      final String workloadType) {
    return () -> {
      ready.countDown();
      if (!start.await(timeoutSeconds, TimeUnit.SECONDS)) {
        throw new IllegalStateException("benchmark start signal timed out");
      }

      long stopAtNs = System.nanoTime() + durationNs;
      int completedInvocations = 0;
      List<LatencySample> localSamples = new ArrayList<>();
      while (System.nanoTime() < stopAtNs) {
        if (Thread.currentThread().isInterrupted()) {
          throw new InterruptedException("benchmark caller interrupted");
        }
        long beginNs = System.nanoTime();
        executeSqlAndCheck(pool, statement);
        long requestId = requestIdGenerator.incrementAndGet();
        double latencyMs = (System.nanoTime() - beginNs) / 1_000_000.0d;
        localSamples.add(
            new LatencySample(
                config.architecture,
                config.round,
                scenario,
                workloadType,
                config.concurrency,
                requestId,
                latencyMs));
        completedInvocations++;
      }
      return new TaskMetrics(completedInvocations, localSamples);
    };
  }

  private void awaitReady(CountDownLatch ready, int timeoutSeconds, String target)
      throws Exception {
    if (!ready.await(timeoutSeconds, TimeUnit.SECONDS)) {
      throw new IllegalStateException(target + " failed to get ready in time");
    }
  }

  private List<TaskMetrics> collectTaskMetrics(List<Future<TaskMetrics>> futures) throws Exception {
    List<TaskMetrics> taskMetrics = new ArrayList<>();
    Exception taskFailure = null;
    for (Future<TaskMetrics> future : futures) {
      try {
        taskMetrics.add(future.get());
      } catch (ExecutionException e) {
        if (taskFailure == null) {
          Throwable cause = e.getCause();
          if (cause instanceof Exception) {
            taskFailure = (Exception) cause;
          } else {
            taskFailure = new RuntimeException(cause);
          }
        }
      }
    }
    if (taskFailure != null) {
      throw taskFailure;
    }
    return taskMetrics;
  }

  private void shutdownExecutor(ExecutorService executorService, int timeoutSeconds) {
    executorService.shutdown();
    try {
      if (!executorService.awaitTermination(timeoutSeconds, TimeUnit.SECONDS)) {
        executorService.shutdownNow();
        executorService.awaitTermination(Math.max(1, timeoutSeconds), TimeUnit.SECONDS);
      }
    } catch (InterruptedException e) {
      executorService.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  private String buildUdfStatement(int rows, int loopsPerInvocation) {
    return String.format(
        Locale.ROOT,
        "SELECT %s(*, %d) FROM %s WHERE key < %d;",
        COMPUTE_FUNCTION_NAME,
        loopsPerInvocation,
        SERIES_PREFIX,
        rows);
  }

  private String buildLightStatement(int lightQueryLimit) {
    return String.format(
        Locale.ROOT, "SELECT * FROM %s WHERE key < %d;", SERIES_PREFIX, lightQueryLimit);
  }

  private SessionExecuteSqlResult executeSqlAndCheck(Session session, String statement)
      throws SessionException {
    SessionExecuteSqlResult result = session.executeSql(statement);
    validateSqlResult(statement, result);
    return result;
  }

  private SessionExecuteSqlResult executeSqlAndCheck(SessionPool pool, String statement)
      throws SessionException {
    SessionExecuteSqlResult result = pool.executeSql(statement);
    validateSqlResult(statement, result);
    return result;
  }

  private void validateSqlResult(String statement, SessionExecuteSqlResult result) {
    if (result == null) {
      throw new IllegalStateException("null sql result for statement: " + statement);
    }
    if (result.getParseErrorMsg() != null && !result.getParseErrorMsg().trim().isEmpty()) {
      throw new IllegalStateException(
          "statement failed: " + statement + ", error=" + result.getParseErrorMsg());
    }
  }

  private void safeExecuteSql(Session session, String statement) {
    try {
      session.executeSql(statement);
    } catch (Exception ignored) {
      // Cleanup operations intentionally ignore failures.
    }
  }

  private void cleanup(Session session) {
    try {
      safeExecuteSql(session, String.format(DROP_SQL_TEMPLATE, COMPUTE_FUNCTION_NAME));
      safeExecuteSql(session, String.format(DROP_SQL_TEMPLATE, META_FUNCTION_NAME));
      safeExecuteSql(session, "clear data;");
    } finally {
      try {
        session.closeSession();
      } catch (SessionException ignored) {
        // Ignore close failures during cleanup.
      }
    }
  }

  private void appendThroughputCsv(Path outputCsv, List<BenchmarkRecord> records)
      throws IOException {
    if (records.isEmpty()) {
      return;
    }
    Path parent = outputCsv.toAbsolutePath().getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }
    boolean writeHeader = !Files.exists(outputCsv);
    List<String> lines = new ArrayList<>();
    if (writeHeader) {
      lines.add(THROUGHPUT_CSV_HEADER);
    }
    for (BenchmarkRecord record : records) {
      lines.add(record.toCsvLine());
    }
    Files.write(
        outputCsv,
        lines,
        StandardCharsets.UTF_8,
        StandardOpenOption.CREATE,
        StandardOpenOption.APPEND);
  }

  private void appendLatencyCsv(Path outputCsv, List<LatencySample> samples) throws IOException {
    if (samples.isEmpty()) {
      return;
    }
    Path parent = outputCsv.toAbsolutePath().getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }
    boolean writeHeader = !Files.exists(outputCsv);
    List<String> lines = new ArrayList<>();
    if (writeHeader) {
      lines.add(LATENCY_CSV_HEADER);
    }
    for (LatencySample sample : samples) {
      lines.add(sample.toCsvLine());
    }
    Files.write(
        outputCsv,
        lines,
        StandardCharsets.UTF_8,
        StandardOpenOption.CREATE,
        StandardOpenOption.APPEND);
  }

  private static String csvEscape(String value) {
    if (value == null) {
      return "\"\"";
    }
    return "\"" + value.replace("\"", "\"\"") + "\"";
  }

  private static String formatDouble(double value) {
    return String.format(Locale.ROOT, "%.4f", value);
  }

  private static class BenchmarkConfig {
    private final String host;
    private final int port;
    private final String username;
    private final String password;
    private final String architecture;
    private final int round;
    private final int concurrency;
    private final int rows;
    private final int cols;
    private final int loopsPerInvocation;
    private final int initialThreads;
    private final int minThreads;
    private final int maxThreads;
    private final int steadyInvocationsPerThread;
    private final int warmupRounds;
    private final int mixedDurationSeconds;
    private final int lightQueryConcurrency;
    private final int lightQueryLimit;
    private final int timeoutSeconds;
    private final String pythonExec;
    private final String udfScriptPath;
    private final Path throughputCsv;
    private final Path latencyCsv;

    private BenchmarkConfig(
        String host,
        int port,
        String username,
        String password,
        String architecture,
        int round,
        int concurrency,
        int rows,
        int cols,
        int loopsPerInvocation,
        int initialThreads,
        int minThreads,
        int maxThreads,
        int steadyInvocationsPerThread,
        int warmupRounds,
        int mixedDurationSeconds,
        int lightQueryConcurrency,
        int lightQueryLimit,
        int timeoutSeconds,
        String pythonExec,
        String udfScriptPath,
        Path throughputCsv,
        Path latencyCsv) {
      this.host = host;
      this.port = port;
      this.username = username;
      this.password = password;
      this.architecture = architecture;
      this.round = round;
      this.concurrency = concurrency;
      this.rows = rows;
      this.cols = cols;
      this.loopsPerInvocation = loopsPerInvocation;
      this.initialThreads = initialThreads;
      this.minThreads = minThreads;
      this.maxThreads = maxThreads;
      this.steadyInvocationsPerThread = steadyInvocationsPerThread;
      this.warmupRounds = warmupRounds;
      this.mixedDurationSeconds = mixedDurationSeconds;
      this.lightQueryConcurrency = lightQueryConcurrency;
      this.lightQueryLimit = lightQueryLimit;
      this.timeoutSeconds = timeoutSeconds;
      this.pythonExec = pythonExec;
      this.udfScriptPath = udfScriptPath;
      this.throughputCsv = throughputCsv;
      this.latencyCsv = latencyCsv;
    }

    private static BenchmarkConfig fromArgs(String[] args) {
      Map<String, String> values = parseArgs(args);
      String host = values.getOrDefault("host", "127.0.0.1");
      int port = Integer.parseInt(values.getOrDefault("port", "6888"));
      String username = values.getOrDefault("user", "root");
      String password = values.getOrDefault("password", "root");
      String architecture = values.getOrDefault("architecture", "unknown");
      int round = Integer.parseInt(values.getOrDefault("round", "1"));
      int concurrency = Integer.parseInt(values.getOrDefault("concurrency", "4"));
      int rows = Integer.parseInt(values.getOrDefault("rows", "10000"));
      int cols = Integer.parseInt(values.getOrDefault("cols", "10"));
      int loopsPerInvocation = Integer.parseInt(values.getOrDefault("loops", "20"));
      int initialThreads = Integer.parseInt(values.getOrDefault("initial-threads", "4"));
      int minThreads = Integer.parseInt(values.getOrDefault("min-threads", "4"));
      int maxThreads = Integer.parseInt(values.getOrDefault("max-threads", "8"));
      int steadyInvocationsPerThread =
          Integer.parseInt(values.getOrDefault("steady-invocations-per-thread", "10"));
      int warmupRounds = Integer.parseInt(values.getOrDefault("warmup", "1"));
      int mixedDurationSeconds =
          Integer.parseInt(values.getOrDefault("mixed-duration-seconds", "30"));
      int lightQueryConcurrency =
          Integer.parseInt(values.getOrDefault("light-query-concurrency", "2"));
      int lightQueryLimit = Integer.parseInt(values.getOrDefault("light-query-limit", "256"));
      int timeoutSeconds =
          Integer.parseInt(
              values.getOrDefault("timeout-seconds", String.valueOf(DEFAULT_TIMEOUT_SECONDS)));
      String pythonExec = values.get("python-exec");
      String udfDir = values.getOrDefault("udf-dir", "benchmark" + File.separator + "udf");
      File udfBaseDir = new File(udfDir).getAbsoluteFile();
      Path throughputCsv =
          Paths.get(
              values.getOrDefault(
                  "throughput-csv", "benchmark/results/architecture/raw-throughput.csv"));
      Path latencyCsv =
          Paths.get(
              values.getOrDefault(
                  "latency-csv", "benchmark/results/architecture/raw-latency-samples.csv"));
      return new BenchmarkConfig(
          host,
          port,
          username,
          password,
          architecture,
          round,
          concurrency,
          rows,
          cols,
          loopsPerInvocation,
          initialThreads,
          minThreads,
          maxThreads,
          steadyInvocationsPerThread,
          warmupRounds,
          mixedDurationSeconds,
          lightQueryConcurrency,
          lightQueryLimit,
          timeoutSeconds,
          pythonExec,
          new File(udfBaseDir, UDF_SCRIPT_NAME).getAbsolutePath(),
          throughputCsv.toAbsolutePath(),
          latencyCsv.toAbsolutePath());
    }

    private static Map<String, String> parseArgs(String[] args) {
      Map<String, String> values = new LinkedHashMap<>();
      for (int i = 0; i < args.length; i++) {
        String arg = args[i];
        if (!arg.startsWith("--")) {
          throw new IllegalArgumentException("unexpected argument: " + arg);
        }
        String key = arg.substring(2);
        if (i + 1 >= args.length || args[i + 1].startsWith("--")) {
          throw new IllegalArgumentException("missing value for argument: " + arg);
        }
        values.put(key, args[++i]);
      }
      return values;
    }
  }

  private static class PythonModeInfo {
    private final String gilEnabled;
    private final String pythonVersion;

    private PythonModeInfo(String gilEnabled, String pythonVersion) {
      this.gilEnabled = gilEnabled;
      this.pythonVersion = pythonVersion;
    }
  }

  private static class TaskMetrics {
    private final int completedInvocations;
    private final List<LatencySample> samples;

    private TaskMetrics(int completedInvocations, List<LatencySample> samples) {
      this.completedInvocations = completedInvocations;
      this.samples = samples;
    }
  }

  private static class ScenarioResult {
    private final BenchmarkRecord record;
    private final List<LatencySample> latencySamples;

    private ScenarioResult(BenchmarkRecord record, List<LatencySample> latencySamples) {
      this.record = record;
      this.latencySamples = latencySamples;
    }
  }

  private static class BenchmarkRecord {
    private final String architecture;
    private final int round;
    private final String scenario;
    private final int concurrency;
    private final int rows;
    private final int cols;
    private final int loopsPerInvocation;
    private final int initialThreads;
    private final int minThreads;
    private final int maxThreads;
    private final int steadyInvocationsPerThread;
    private final int mixedDurationSeconds;
    private final int lightQueryConcurrency;
    private final int lightQueryLimit;
    private final long totalInvocations;
    private final double elapsedMs;
    private final double throughput;
    private final String pythonVersion;
    private final String gilEnabled;
    private final String status;
    private final String error;

    private BenchmarkRecord(
        String architecture,
        int round,
        String scenario,
        int concurrency,
        int rows,
        int cols,
        int loopsPerInvocation,
        int initialThreads,
        int minThreads,
        int maxThreads,
        int steadyInvocationsPerThread,
        int mixedDurationSeconds,
        int lightQueryConcurrency,
        int lightQueryLimit,
        long totalInvocations,
        double elapsedMs,
        double throughput,
        String pythonVersion,
        String gilEnabled,
        String status,
        String error) {
      this.architecture = architecture;
      this.round = round;
      this.scenario = scenario;
      this.concurrency = concurrency;
      this.rows = rows;
      this.cols = cols;
      this.loopsPerInvocation = loopsPerInvocation;
      this.initialThreads = initialThreads;
      this.minThreads = minThreads;
      this.maxThreads = maxThreads;
      this.steadyInvocationsPerThread = steadyInvocationsPerThread;
      this.mixedDurationSeconds = mixedDurationSeconds;
      this.lightQueryConcurrency = lightQueryConcurrency;
      this.lightQueryLimit = lightQueryLimit;
      this.totalInvocations = totalInvocations;
      this.elapsedMs = elapsedMs;
      this.throughput = throughput;
      this.pythonVersion = pythonVersion;
      this.gilEnabled = gilEnabled;
      this.status = status;
      this.error = error;
    }

    private static BenchmarkRecord success(
        BenchmarkConfig config,
        PythonModeInfo pythonMode,
        String scenario,
        long totalInvocations,
        double elapsedMs,
        double throughput) {
      return new BenchmarkRecord(
          config.architecture,
          config.round,
          scenario,
          config.concurrency,
          config.rows,
          config.cols,
          config.loopsPerInvocation,
          config.initialThreads,
          config.minThreads,
          config.maxThreads,
          config.steadyInvocationsPerThread,
          config.mixedDurationSeconds,
          config.lightQueryConcurrency,
          config.lightQueryLimit,
          totalInvocations,
          elapsedMs,
          throughput,
          pythonMode.pythonVersion,
          pythonMode.gilEnabled,
          "ok",
          "");
    }

    private static BenchmarkRecord failure(
        BenchmarkConfig config, PythonModeInfo pythonMode, String scenario, Exception e) {
      return new BenchmarkRecord(
          config.architecture,
          config.round,
          scenario,
          config.concurrency,
          config.rows,
          config.cols,
          config.loopsPerInvocation,
          config.initialThreads,
          config.minThreads,
          config.maxThreads,
          config.steadyInvocationsPerThread,
          config.mixedDurationSeconds,
          config.lightQueryConcurrency,
          config.lightQueryLimit,
          0L,
          0.0d,
          0.0d,
          pythonMode.pythonVersion,
          pythonMode.gilEnabled,
          "failed",
          e.getMessage() == null ? e.toString() : e.getMessage().replace('\n', ' '));
    }

    private String toCsvLine() {
      return String.join(
          ",",
          csvEscape(architecture),
          String.valueOf(round),
          csvEscape(scenario),
          String.valueOf(concurrency),
          String.valueOf(rows),
          String.valueOf(cols),
          String.valueOf(loopsPerInvocation),
          String.valueOf(initialThreads),
          String.valueOf(minThreads),
          String.valueOf(maxThreads),
          String.valueOf(steadyInvocationsPerThread),
          String.valueOf(mixedDurationSeconds),
          String.valueOf(lightQueryConcurrency),
          String.valueOf(lightQueryLimit),
          String.valueOf(totalInvocations),
          formatDouble(elapsedMs),
          formatDouble(throughput),
          csvEscape(pythonVersion),
          csvEscape(gilEnabled),
          csvEscape(status),
          csvEscape(error));
    }
  }

  private static class LatencySample {
    private final String architecture;
    private final int round;
    private final String scenario;
    private final String workloadType;
    private final int concurrency;
    private final long requestId;
    private final double latencyMs;

    private LatencySample(
        String architecture,
        int round,
        String scenario,
        String workloadType,
        int concurrency,
        long requestId,
        double latencyMs) {
      this.architecture = architecture;
      this.round = round;
      this.scenario = scenario;
      this.workloadType = workloadType;
      this.concurrency = concurrency;
      this.requestId = requestId;
      this.latencyMs = latencyMs;
    }

    private String toCsvLine() {
      return String.join(
          ",",
          csvEscape(architecture),
          String.valueOf(round),
          csvEscape(scenario),
          csvEscape(workloadType),
          String.valueOf(concurrency),
          String.valueOf(requestId),
          formatDouble(latencyMs));
    }
  }
}
