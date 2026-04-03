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
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Dynamic benchmark runner for observing adaptive Python UDF pool behavior under a five-phase load
 * timeline. It produces per-second client-side time series and relies on server-side scheduler logs
 * for pool and EWMA metrics.
 */
public class AdaptiveUdfArchitectureBenchmarkRunner {

  private static final int DEFAULT_TIMEOUT_SECONDS = 30;
  private static final String SERIES_PREFIX = "bench.arch";
  private static final String COMPUTE_FUNCTION_NAME = "arch_latency_udf_bench";
  private static final String META_FUNCTION_NAME = "arch_meta_bench";
  private static final String META_UDF_SCRIPT_NAME = "architecture_benchmark_metadata_udsf.py";
  private static final String COMPUTE_UDF_SCRIPT_NAME = "architecture_benchmark_compute_udsf.py";
  private static final String REGISTER_SQL_TEMPLATE =
      "CREATE FUNCTION UDSF \"%s\" FROM \"%s\" IN \"%s\";";
  private static final String DROP_SQL_TEMPLATE = "DROP FUNCTION \"%s\";";
  private static final String TIMESERIES_CSV_HEADER =
      "round,timestampSec,sampleTimeEpochMs,experimentStartEpochMs,phase,targetConcurrency,"
          + "completedRequests,throughputPerSecond,udfP50,udfP95,rows,cols,loopsPerInvocation,"
          + "initialThreads,minThreads,maxThreads,pythonVersion,gilEnabled,status,error";
  private static final List<String> DEFAULT_PHASE_NAMES =
      Arrays.asList(
          "phase1_warmup", "phase2_peak", "phase3_recovery", "phase4_rebound", "phase5_cooldown");

  public static void main(String[] args) throws Exception {
    BenchmarkConfig config = BenchmarkConfig.fromArgs(args);
    new AdaptiveUdfArchitectureBenchmarkRunner().run(config);
  }

  private void run(BenchmarkConfig config) throws Exception {
    logProgress(
        config,
        "benchmark start, rows=%d, cols=%d, loops=%d, totalDuration=%ds, phases=%s",
        config.rows,
        config.cols,
        config.loopsPerInvocation,
        config.getTotalDurationSeconds(),
        config.describePhases());
    Session controlSession =
        new Session(config.host, config.port, config.username, config.password);
    List<TimeSeriesSample> samples = new ArrayList<>();
    PythonModeInfo pythonMode = PythonModeInfo.unknown();
    Exception benchmarkFailure = null;
    boolean sessionOpened = false;
    try {
      logProgress(config, "opening control session to %s:%d", config.host, config.port);
      controlSession.openSession();
      sessionOpened = true;
      logProgress(config, "control session opened");
      logProgress(config, "registering benchmark UDFs");
      registerFunctions(controlSession, config);
      logProgress(config, "benchmark UDFs registered");
      logProgress(config, "inserting benchmark data");
      insertData(controlSession, config.rows, config.cols);
      logProgress(config, "benchmark data inserted");
      logProgress(config, "probing python runtime metadata");
      pythonMode = fetchPythonMode(controlSession, config);
      logProgress(
          config,
          "python runtime ready, version=%s, gilEnabled=%s",
          pythonMode.pythonVersion,
          pythonMode.gilEnabled);
      warmUp(config);
      logProgress(config, "warmup finished, entering dynamic five-phase scenario");
      samples.addAll(runDynamicScenario(config, pythonMode));
      logProgress(
          config, "dynamic scenario finished, collected %d second-level samples", samples.size());
    } catch (Exception e) {
      benchmarkFailure = e;
      logProgress(
          config, "benchmark failed: %s", e.getMessage() == null ? e.toString() : e.getMessage());
      samples.add(TimeSeriesSample.failure(config, pythonMode, e));
    } finally {
      if (sessionOpened) {
        logProgress(config, "cleaning benchmark functions and data");
        cleanup(controlSession);
      }
      logProgress(config, "writing time series csv to %s", config.timeSeriesCsv);
      appendTimeSeriesCsv(config.timeSeriesCsv, samples);
      logProgress(config, "time series csv written");
    }
    if (benchmarkFailure != null) {
      throw benchmarkFailure;
    }
    logProgress(config, "benchmark completed successfully");
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
            config.metadataUdfScriptPath));
    executeSqlAndCheck(
        session,
        String.format(
            REGISTER_SQL_TEMPLATE,
            COMPUTE_FUNCTION_NAME,
            "ArchitectureComputeIntensiveBenchmark",
            config.computeUdfScriptPath));
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
                "SELECT %s(*, 0) FROM %s WHERE key < 1;", META_FUNCTION_NAME, SERIES_PREFIX));
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
            Math.max(config.getMaxPhaseConcurrency() * 2, 8));
    String udfStatement = buildUdfStatement(config.rows, config.loopsPerInvocation);
    int warmupConcurrency =
        Math.max(config.initialThreads, config.getPhaseForSecond(0).targetConcurrency);
    logProgress(
        config,
        "warmup start, rounds=%d, concurrency=%d, invocationsPerThread=%d, settleMs=%d",
        Math.max(1, config.warmupRounds),
        warmupConcurrency,
        config.warmupInvocationsPerThread,
        config.warmupSettleMs);
    try {
      for (int i = 0; i < Math.max(1, config.warmupRounds); i++) {
        logProgress(config, "warmup round %d/%d running", i + 1, Math.max(1, config.warmupRounds));
        runConcurrentWarmup(
            pool,
            udfStatement,
            warmupConcurrency,
            config.warmupInvocationsPerThread,
            config.timeoutSeconds,
            "udf warmup");
        if (config.warmupSettleMs > 0) {
          logProgress(config, "warmup round %d settle for %d ms", i + 1, config.warmupSettleMs);
          Thread.sleep(config.warmupSettleMs);
        }
      }
    } finally {
      pool.close();
    }
    logProgress(config, "warmup completed");
  }

  private void runConcurrentWarmup(
      SessionPool pool,
      String statement,
      int concurrency,
      int invocationsPerThread,
      int timeoutSeconds,
      String target)
      throws Exception {
    ExecutorService callers = Executors.newFixedThreadPool(Math.max(1, concurrency));
    CountDownLatch ready = new CountDownLatch(Math.max(1, concurrency));
    CountDownLatch start = new CountDownLatch(1);
    List<Future<Void>> futures = new ArrayList<>();

    try {
      for (int i = 0; i < Math.max(1, concurrency); i++) {
        futures.add(
            callers.submit(
                () -> {
                  ready.countDown();
                  if (!start.await(timeoutSeconds, TimeUnit.SECONDS)) {
                    throw new IllegalStateException(target + " start signal timed out");
                  }
                  for (int j = 0; j < Math.max(1, invocationsPerThread); j++) {
                    executeSqlAndCheck(pool, statement);
                  }
                  return null;
                }));
      }

      awaitReady(ready, timeoutSeconds, target);
      start.countDown();
      collectVoidTasks(futures);
    } finally {
      shutdownExecutor(callers, timeoutSeconds);
    }
  }

  private List<TimeSeriesSample> runDynamicScenario(
      BenchmarkConfig config, PythonModeInfo pythonMode) throws Exception {
    String statement = buildUdfStatement(config.rows, config.loopsPerInvocation);
    int maxConcurrency = config.getMaxPhaseConcurrency();
    SessionPool pool =
        new SessionPool(
            config.host,
            config.port,
            config.username,
            config.password,
            Math.max(maxConcurrency * 2, 8));
    ExecutorService callers = Executors.newFixedThreadPool(maxConcurrency);
    CountDownLatch ready = new CountDownLatch(maxConcurrency);
    CountDownLatch start = new CountDownLatch(1);
    AtomicBoolean running = new AtomicBoolean(true);
    AtomicInteger targetConcurrency =
        new AtomicInteger(config.getPhaseForSecond(0).targetConcurrency);
    AtomicReference<Exception> taskFailure = new AtomicReference<>();
    CompletionRecorder recorder = new CompletionRecorder();
    List<Future<Void>> futures = new ArrayList<>();
    logProgress(
        config,
        "dynamic scenario preparing workers, maxConcurrency=%d, totalDuration=%ds",
        maxConcurrency,
        config.getTotalDurationSeconds());

    try {
      for (int workerIndex = 0; workerIndex < maxConcurrency; workerIndex++) {
        futures.add(
            callers.submit(
                createDynamicLoadTask(
                    workerIndex,
                    pool,
                    statement,
                    ready,
                    start,
                    config.timeoutSeconds,
                    running,
                    targetConcurrency,
                    recorder,
                    taskFailure)));
      }

      awaitReady(ready, config.timeoutSeconds, "dynamic benchmark callers");
      logProgress(config, "all dynamic workers ready, starting scenario");
      long experimentStartEpochMs = System.currentTimeMillis();
      long experimentStartNs = System.nanoTime();
      start.countDown();

      List<TimeSeriesSample> samples =
          collectTimeSeriesSamples(
              config,
              pythonMode,
              experimentStartEpochMs,
              experimentStartNs,
              targetConcurrency,
              recorder,
              taskFailure);

      running.set(false);
      collectVoidTasks(futures);
      logProgress(config, "all dynamic workers completed");
      return samples;
    } finally {
      running.set(false);
      shutdownExecutor(callers, config.timeoutSeconds);
      pool.close();
    }
  }

  private Callable<Void> createDynamicLoadTask(
      final int workerIndex,
      final SessionPool pool,
      final String statement,
      final CountDownLatch ready,
      final CountDownLatch start,
      final int timeoutSeconds,
      final AtomicBoolean running,
      final AtomicInteger targetConcurrency,
      final CompletionRecorder recorder,
      final AtomicReference<Exception> taskFailure) {
    return () -> {
      ready.countDown();
      if (!start.await(timeoutSeconds, TimeUnit.SECONDS)) {
        throw new IllegalStateException("benchmark start signal timed out");
      }

      while (running.get()) {
        Exception failure = taskFailure.get();
        if (failure != null) {
          throw failure;
        }
        if (Thread.currentThread().isInterrupted()) {
          throw new InterruptedException("benchmark caller interrupted");
        }
        if (workerIndex >= targetConcurrency.get()) {
          Thread.sleep(20L);
          continue;
        }
        try {
          long beginNs = System.nanoTime();
          executeSqlAndCheck(pool, statement);
          recorder.record(System.currentTimeMillis(), (System.nanoTime() - beginNs) / 1_000_000.0d);
        } catch (Exception e) {
          taskFailure.compareAndSet(null, e);
          throw e;
        }
      }
      return null;
    };
  }

  private List<TimeSeriesSample> collectTimeSeriesSamples(
      BenchmarkConfig config,
      PythonModeInfo pythonMode,
      long experimentStartEpochMs,
      long experimentStartNs,
      AtomicInteger targetConcurrency,
      CompletionRecorder recorder,
      AtomicReference<Exception> taskFailure)
      throws Exception {
    List<TimeSeriesSample> samples = new ArrayList<>();
    String currentPhaseName = null;
    for (int secondIndex = 0; secondIndex < config.getTotalDurationSeconds(); secondIndex++) {
      PhaseConfig phase = config.getPhaseForSecond(secondIndex);
      if (!phase.name.equals(currentPhaseName)) {
        currentPhaseName = phase.name;
        logProgress(
            config,
            "enter phase %s, secondRange=%d-%d, targetConcurrency=%d",
            phase.name,
            phase.startSecond,
            phase.endSecond,
            phase.targetConcurrency);
      }
      targetConcurrency.set(phase.targetConcurrency);
      sleepUntil(
          experimentStartNs + TimeUnit.SECONDS.toNanos(secondIndex + 1L),
          taskFailure,
          config.timeoutSeconds);
      long sampleTimeEpochMs = System.currentTimeMillis();
      List<CompletedInvocation> invocations = recorder.drain();
      List<Double> latencies = new ArrayList<>(invocations.size());
      for (CompletedInvocation invocation : invocations) {
        latencies.add(invocation.latencyMs);
      }
      double udfP50 = percentile(latencies, 0.50);
      double udfP95 = percentile(latencies, 0.95);
      samples.add(
          TimeSeriesSample.success(
              config,
              pythonMode,
              secondIndex + 1,
              sampleTimeEpochMs,
              experimentStartEpochMs,
              phase.name,
              phase.targetConcurrency,
              invocations.size(),
              udfP50,
              udfP95));
      logProgress(
          config,
          "progress %d/%ds, phase=%s, targetConcurrency=%d, completed=%d, throughput=%.2f qps, udfP50=%.2f ms, udfP95=%.2f ms",
          secondIndex + 1,
          config.getTotalDurationSeconds(),
          phase.name,
          phase.targetConcurrency,
          invocations.size(),
          (double) invocations.size(),
          udfP50,
          udfP95);
    }
    Exception failure = taskFailure.get();
    if (failure != null) {
      throw failure;
    }
    return samples;
  }

  private void sleepUntil(
      long deadlineNs, AtomicReference<Exception> taskFailure, int timeoutSeconds)
      throws Exception {
    long hardDeadlineNs =
        System.nanoTime() + TimeUnit.SECONDS.toNanos(Math.max(1L, timeoutSeconds * 2L));
    while (true) {
      Exception failure = taskFailure.get();
      if (failure != null) {
        throw failure;
      }
      long remainingNs = deadlineNs - System.nanoTime();
      if (remainingNs <= 0) {
        return;
      }
      if (System.nanoTime() > hardDeadlineNs) {
        throw new IllegalStateException("timed out while waiting for the next sample window");
      }
      long sleepMs = Math.max(1L, Math.min(TimeUnit.NANOSECONDS.toMillis(remainingNs), 200L));
      Thread.sleep(sleepMs);
    }
  }

  private void awaitReady(CountDownLatch ready, int timeoutSeconds, String target)
      throws Exception {
    if (!ready.await(timeoutSeconds, TimeUnit.SECONDS)) {
      throw new IllegalStateException(target + " failed to get ready in time");
    }
  }

  private void collectVoidTasks(List<Future<Void>> futures) throws Exception {
    Exception taskFailure = null;
    for (Future<Void> future : futures) {
      try {
        future.get();
      } catch (ExecutionException e) {
        if (taskFailure == null) {
          Throwable cause = e.getCause();
          taskFailure =
              cause instanceof Exception ? (Exception) cause : new RuntimeException(cause);
        }
      }
    }
    if (taskFailure != null) {
      throw taskFailure;
    }
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

  private void appendTimeSeriesCsv(Path outputCsv, List<TimeSeriesSample> samples)
      throws IOException {
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
      lines.add(TIMESERIES_CSV_HEADER);
    }
    for (TimeSeriesSample sample : samples) {
      lines.add(sample.toCsvLine());
    }
    Files.write(
        outputCsv,
        lines,
        StandardCharsets.UTF_8,
        StandardOpenOption.CREATE,
        StandardOpenOption.APPEND);
  }

  private static double percentile(List<Double> values, double p) {
    if (values.isEmpty()) {
      return 0.0d;
    }
    values.sort(Double::compareTo);
    if (values.size() == 1) {
      return values.get(0);
    }
    double position = (values.size() - 1) * p;
    int lower = (int) Math.floor(position);
    int upper = (int) Math.ceil(position);
    if (lower == upper) {
      return values.get(lower);
    }
    double weight = position - lower;
    return values.get(lower) * (1.0d - weight) + values.get(upper) * weight;
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

  private void logProgress(BenchmarkConfig config, String template, Object... args) {
    String message = String.format(Locale.ROOT, template, args);
    System.out.printf(
        Locale.ROOT,
        "[AdaptiveUdfArchitectureBenchmarkRunner][round=%d] %s%n",
        config.round,
        message);
    System.out.flush();
  }

  private static class BenchmarkConfig {
    private final String host;
    private final int port;
    private final String username;
    private final String password;
    private final int round;
    private final int rows;
    private final int cols;
    private final int loopsPerInvocation;
    private final int initialThreads;
    private final int minThreads;
    private final int maxThreads;
    private final int warmupRounds;
    private final int warmupInvocationsPerThread;
    private final long warmupSettleMs;
    private final int timeoutSeconds;
    private final String pythonExec;
    private final String metadataUdfScriptPath;
    private final String computeUdfScriptPath;
    private final Path timeSeriesCsv;
    private final List<PhaseConfig> phases;

    private BenchmarkConfig(
        String host,
        int port,
        String username,
        String password,
        int round,
        int rows,
        int cols,
        int loopsPerInvocation,
        int initialThreads,
        int minThreads,
        int maxThreads,
        int warmupRounds,
        int warmupInvocationsPerThread,
        long warmupSettleMs,
        int timeoutSeconds,
        String pythonExec,
        String metadataUdfScriptPath,
        String computeUdfScriptPath,
        Path timeSeriesCsv,
        List<PhaseConfig> phases) {
      this.host = host;
      this.port = port;
      this.username = username;
      this.password = password;
      this.round = round;
      this.rows = rows;
      this.cols = cols;
      this.loopsPerInvocation = loopsPerInvocation;
      this.initialThreads = initialThreads;
      this.minThreads = minThreads;
      this.maxThreads = maxThreads;
      this.warmupRounds = warmupRounds;
      this.warmupInvocationsPerThread = warmupInvocationsPerThread;
      this.warmupSettleMs = warmupSettleMs;
      this.timeoutSeconds = timeoutSeconds;
      this.pythonExec = pythonExec;
      this.metadataUdfScriptPath = metadataUdfScriptPath;
      this.computeUdfScriptPath = computeUdfScriptPath;
      this.timeSeriesCsv = timeSeriesCsv;
      this.phases = phases;
    }

    private int getMaxPhaseConcurrency() {
      int max = 1;
      for (PhaseConfig phase : phases) {
        max = Math.max(max, phase.targetConcurrency);
      }
      return max;
    }

    private int getTotalDurationSeconds() {
      return phases.isEmpty() ? 0 : phases.get(phases.size() - 1).endSecond;
    }

    private String describePhases() {
      StringBuilder builder = new StringBuilder();
      for (int i = 0; i < phases.size(); i++) {
        if (i > 0) {
          builder.append(" | ");
        }
        PhaseConfig phase = phases.get(i);
        builder
            .append(phase.name)
            .append('(')
            .append(phase.startSecond)
            .append('-')
            .append(phase.endSecond)
            .append("s,c=")
            .append(phase.targetConcurrency)
            .append(')');
      }
      return builder.toString();
    }

    private PhaseConfig getPhaseForSecond(int secondIndex) {
      for (PhaseConfig phase : phases) {
        if (secondIndex >= phase.startSecond && secondIndex < phase.endSecond) {
          return phase;
        }
      }
      return phases.get(phases.size() - 1);
    }

    private static BenchmarkConfig fromArgs(String[] args) {
      Map<String, String> values = parseArgs(args);
      String host = values.getOrDefault("host", "127.0.0.1");
      int port = Integer.parseInt(values.getOrDefault("port", "6888"));
      String username = values.getOrDefault("user", "root");
      String password = values.getOrDefault("password", "root");
      int round = Integer.parseInt(values.getOrDefault("round", "1"));
      int rows = Integer.parseInt(values.getOrDefault("rows", "10000"));
      int cols = Integer.parseInt(values.getOrDefault("cols", "10"));
      int loopsPerInvocation = Integer.parseInt(values.getOrDefault("loops", "20"));
      int initialThreads = Integer.parseInt(values.getOrDefault("initial-threads", "4"));
      int minThreads = Integer.parseInt(values.getOrDefault("min-threads", "4"));
      int maxThreads = Integer.parseInt(values.getOrDefault("max-threads", "8"));
      int warmupRounds = Integer.parseInt(values.getOrDefault("warmup", "1"));
      int warmupInvocationsPerThread =
          Integer.parseInt(values.getOrDefault("warmup-invocations-per-thread", "2"));
      long warmupSettleMs = Long.parseLong(values.getOrDefault("warmup-settle-ms", "2500"));
      int timeoutSeconds =
          Integer.parseInt(
              values.getOrDefault("timeout-seconds", String.valueOf(DEFAULT_TIMEOUT_SECONDS)));
      String pythonExec = values.get("python-exec");
      String udfDir = values.getOrDefault("udf-dir", "benchmark" + File.separator + "udf");
      File udfBaseDir = new File(udfDir).getAbsoluteFile();
      Path timeSeriesCsv =
          Paths.get(
              values.getOrDefault(
                  "timeseries-csv", "benchmark/results/architecture/adaptive-timeseries.csv"));
      List<PhaseConfig> phases =
          parsePhases(
              values.getOrDefault("phase-durations", "30,60,40,30,50"),
              values.getOrDefault("phase-concurrencies", "4,32,4,24,2"));

      return new BenchmarkConfig(
          host,
          port,
          username,
          password,
          round,
          rows,
          cols,
          loopsPerInvocation,
          initialThreads,
          minThreads,
          maxThreads,
          warmupRounds,
          warmupInvocationsPerThread,
          warmupSettleMs,
          timeoutSeconds,
          pythonExec,
          new File(udfBaseDir, META_UDF_SCRIPT_NAME).getAbsolutePath(),
          new File(udfBaseDir, COMPUTE_UDF_SCRIPT_NAME).getAbsolutePath(),
          timeSeriesCsv.toAbsolutePath(),
          phases);
    }

    private static List<PhaseConfig> parsePhases(String durationsArg, String concurrenciesArg) {
      String[] durationParts = durationsArg.split(",");
      String[] concurrencyParts = concurrenciesArg.split(",");
      if (durationParts.length != concurrencyParts.length) {
        throw new IllegalArgumentException(
            "phase-durations and phase-concurrencies must have the same number of items");
      }
      if (durationParts.length == 0) {
        throw new IllegalArgumentException("at least one phase is required");
      }
      List<PhaseConfig> phases = new ArrayList<>();
      int startSecond = 0;
      for (int i = 0; i < durationParts.length; i++) {
        int duration = Integer.parseInt(durationParts[i].trim());
        int concurrency = Integer.parseInt(concurrencyParts[i].trim());
        if (duration <= 0) {
          throw new IllegalArgumentException("phase duration must be positive: " + duration);
        }
        if (concurrency <= 0) {
          throw new IllegalArgumentException("phase concurrency must be positive: " + concurrency);
        }
        String name =
            i < DEFAULT_PHASE_NAMES.size() ? DEFAULT_PHASE_NAMES.get(i) : "phase" + (i + 1);
        phases.add(new PhaseConfig(name, startSecond, startSecond + duration, concurrency));
        startSecond += duration;
      }
      return phases;
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

  private static class PhaseConfig {
    private final String name;
    private final int startSecond;
    private final int endSecond;
    private final int targetConcurrency;

    private PhaseConfig(String name, int startSecond, int endSecond, int targetConcurrency) {
      this.name = name;
      this.startSecond = startSecond;
      this.endSecond = endSecond;
      this.targetConcurrency = targetConcurrency;
    }
  }

  private static class PythonModeInfo {
    private final String gilEnabled;
    private final String pythonVersion;

    private PythonModeInfo(String gilEnabled, String pythonVersion) {
      this.gilEnabled = gilEnabled;
      this.pythonVersion = pythonVersion;
    }

    private static PythonModeInfo unknown() {
      return new PythonModeInfo("unknown", "unknown");
    }
  }

  private static class CompletionRecorder {
    private final ConcurrentLinkedQueue<CompletedInvocation> completedInvocations =
        new ConcurrentLinkedQueue<>();

    private void record(long completedAtEpochMs, double latencyMs) {
      completedInvocations.add(new CompletedInvocation(latencyMs));
    }

    private List<CompletedInvocation> drain() {
      List<CompletedInvocation> drained = new ArrayList<>();
      CompletedInvocation current;
      while ((current = completedInvocations.poll()) != null) {
        drained.add(current);
      }
      return drained;
    }
  }

  private static class CompletedInvocation {
    private final double latencyMs;

    private CompletedInvocation(double latencyMs) {
      this.latencyMs = latencyMs;
    }
  }

  private static class TimeSeriesSample {
    private final int round;
    private final int timestampSec;
    private final long sampleTimeEpochMs;
    private final long experimentStartEpochMs;
    private final String phase;
    private final int targetConcurrency;
    private final int completedRequests;
    private final double throughputPerSecond;
    private final double udfP50;
    private final double udfP95;
    private final int rows;
    private final int cols;
    private final int loopsPerInvocation;
    private final int initialThreads;
    private final int minThreads;
    private final int maxThreads;
    private final String pythonVersion;
    private final String gilEnabled;
    private final String status;
    private final String error;

    private TimeSeriesSample(
        int round,
        int timestampSec,
        long sampleTimeEpochMs,
        long experimentStartEpochMs,
        String phase,
        int targetConcurrency,
        int completedRequests,
        double throughputPerSecond,
        double udfP50,
        double udfP95,
        int rows,
        int cols,
        int loopsPerInvocation,
        int initialThreads,
        int minThreads,
        int maxThreads,
        String pythonVersion,
        String gilEnabled,
        String status,
        String error) {
      this.round = round;
      this.timestampSec = timestampSec;
      this.sampleTimeEpochMs = sampleTimeEpochMs;
      this.experimentStartEpochMs = experimentStartEpochMs;
      this.phase = phase;
      this.targetConcurrency = targetConcurrency;
      this.completedRequests = completedRequests;
      this.throughputPerSecond = throughputPerSecond;
      this.udfP50 = udfP50;
      this.udfP95 = udfP95;
      this.rows = rows;
      this.cols = cols;
      this.loopsPerInvocation = loopsPerInvocation;
      this.initialThreads = initialThreads;
      this.minThreads = minThreads;
      this.maxThreads = maxThreads;
      this.pythonVersion = pythonVersion;
      this.gilEnabled = gilEnabled;
      this.status = status;
      this.error = error;
    }

    private static TimeSeriesSample success(
        BenchmarkConfig config,
        PythonModeInfo pythonMode,
        int timestampSec,
        long sampleTimeEpochMs,
        long experimentStartEpochMs,
        String phase,
        int targetConcurrency,
        int completedRequests,
        double udfP50,
        double udfP95) {
      return new TimeSeriesSample(
          config.round,
          timestampSec,
          sampleTimeEpochMs,
          experimentStartEpochMs,
          phase,
          targetConcurrency,
          completedRequests,
          completedRequests,
          udfP50,
          udfP95,
          config.rows,
          config.cols,
          config.loopsPerInvocation,
          config.initialThreads,
          config.minThreads,
          config.maxThreads,
          pythonMode.pythonVersion,
          pythonMode.gilEnabled,
          "ok",
          "");
    }

    private static TimeSeriesSample failure(
        BenchmarkConfig config, PythonModeInfo pythonMode, Exception e) {
      return new TimeSeriesSample(
          config.round,
          -1,
          System.currentTimeMillis(),
          0L,
          "failed",
          0,
          0,
          0.0d,
          0.0d,
          0.0d,
          config.rows,
          config.cols,
          config.loopsPerInvocation,
          config.initialThreads,
          config.minThreads,
          config.maxThreads,
          pythonMode.pythonVersion,
          pythonMode.gilEnabled,
          "failed",
          e.getMessage() == null ? e.toString() : e.getMessage().replace('\n', ' '));
    }

    private String toCsvLine() {
      return String.join(
          ",",
          String.valueOf(round),
          String.valueOf(timestampSec),
          String.valueOf(sampleTimeEpochMs),
          String.valueOf(experimentStartEpochMs),
          csvEscape(phase),
          String.valueOf(targetConcurrency),
          String.valueOf(completedRequests),
          formatDouble(throughputPerSecond),
          formatDouble(udfP50),
          formatDouble(udfP95),
          String.valueOf(rows),
          String.valueOf(cols),
          String.valueOf(loopsPerInvocation),
          String.valueOf(initialThreads),
          String.valueOf(minThreads),
          String.valueOf(maxThreads),
          csvEscape(pythonVersion),
          csvEscape(gilEnabled),
          csvEscape(status),
          csvEscape(error));
    }
  }
}
