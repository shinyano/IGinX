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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Standalone benchmark runner used by packaged IGinX deployments.
 *
 * <p>This runner assumes IGinX has already been started on the server. It connects through the
 * session API, registers benchmark UDFs, prepares data, runs concurrent queries, and appends raw
 * benchmark records to a csv file.
 */
public class PythonUdfThroughputBenchmarkRunner {

  private static final int DEFAULT_TIMEOUT_SECONDS = 30;
  private static final String SERIES_PREFIX = "bench.d1";
  private static final String PURE_FUNCTION_NAME = "throughput_pure_bench";
  private static final String NUMPY_FUNCTION_NAME = "throughput_numpy_bench";
  private static final String META_FUNCTION_NAME = "throughput_meta_bench";
  private static final String META_SCRIPT_NAME = "benchmark_metadata_udsf.py";
  private static final String PURE_SCRIPT_NAME = "throughput_pure_python_udsf.py";
  private static final String NUMPY_SCRIPT_NAME = "throughput_numpy_udsf.py";
  private static final String REGISTER_SQL_TEMPLATE =
      "CREATE FUNCTION UDSF \"%s\" FROM \"%s\" IN \"%s\";";
  private static final String DROP_SQL_TEMPLATE = "DROP FUNCTION \"%s\";";
  private static final String CSV_HEADER =
      "mode,round,workload,threads,rows,cols,loopsPerInvocation,invocationsPerThread,totalInvocations,elapsedMs,throughput,pythonVersion,gilEnabled,status,error";

  public static void main(String[] args) throws Exception {
    BenchmarkConfig config = BenchmarkConfig.fromArgs(args);
    PythonUdfThroughputBenchmarkRunner runner = new PythonUdfThroughputBenchmarkRunner();
    runner.run(config);
  }

  private void run(BenchmarkConfig config) throws Exception {
    Session controlSession =
        new Session(config.host, config.port, config.username, config.password);
    List<BenchmarkRecord> records = new ArrayList<>();
    boolean sessionOpened = false;
    try {
      controlSession.openSession();
      sessionOpened = true;
      registerFunctions(controlSession, config);
      insertData(controlSession, config.rows, config.cols);
      PythonModeInfo pythonMode = fetchPythonMode(controlSession, config);

      List<Workload> workloads =
          Arrays.asList(
              new Workload("pure_python", PURE_FUNCTION_NAME),
              new Workload("numpy_vectorized", NUMPY_FUNCTION_NAME));
      for (Workload workload : workloads) {
        try {
          warmUp(workload, config);
          BenchmarkMetrics metrics = runBenchmark(workload, config);
          records.add(BenchmarkRecord.success(config, pythonMode, workload.name, metrics));
        } catch (Exception e) {
          records.add(BenchmarkRecord.failure(config, pythonMode, workload.name, e));
        }
      }
    } finally {
      if (sessionOpened) {
        cleanup(controlSession);
      }
      appendCsv(config.outputCsv, records);
    }
  }

  private void registerFunctions(Session session, BenchmarkConfig config) throws SessionException {
    safeExecuteSql(session, String.format(DROP_SQL_TEMPLATE, PURE_FUNCTION_NAME));
    safeExecuteSql(session, String.format(DROP_SQL_TEMPLATE, NUMPY_FUNCTION_NAME));
    safeExecuteSql(session, String.format(DROP_SQL_TEMPLATE, META_FUNCTION_NAME));
    safeExecuteSql(session, "clear data;");

    executeSqlAndCheck(
        session,
        String.format(
            REGISTER_SQL_TEMPLATE,
            META_FUNCTION_NAME,
            "BenchmarkMetadata",
            config.metadataUdfScriptPath));
    executeSqlAndCheck(
        session,
        String.format(
            REGISTER_SQL_TEMPLATE,
            PURE_FUNCTION_NAME,
            "ComputeIntensiveBenchmark",
            config.purePythonUdfScriptPath));
    executeSqlAndCheck(
        session,
        String.format(
            REGISTER_SQL_TEMPLATE,
            NUMPY_FUNCTION_NAME,
            "NumpyBenchmark",
            config.numpyUdfScriptPath));
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
        // Fall back to server-side metadata query when direct probing is unavailable.
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

  private void warmUp(Workload workload, BenchmarkConfig config) throws Exception {
    int warmupRows = Math.min(config.rows, 128);
    int warmupLoops = Math.max(1, Math.min(config.loopsPerInvocation, 2));
    String statement = buildStatement(workload.functionName, warmupRows, warmupLoops);
    SessionPool pool =
        new SessionPool(
            config.host,
            config.port,
            config.username,
            config.password,
            Math.max(1, config.threads));
    try {
      for (int i = 0; i < Math.max(1, config.warmupRounds); i++) {
        executeSqlAndCheck(pool, statement);
      }
    } finally {
      pool.close();
    }
  }

  private BenchmarkMetrics runBenchmark(Workload workload, BenchmarkConfig config)
      throws Exception {
    String statement =
        buildStatement(workload.functionName, config.rows, config.loopsPerInvocation);
    SessionPool pool =
        new SessionPool(
            config.host,
            config.port,
            config.username,
            config.password,
            Math.max(config.threads * 2, config.threads));
    ExecutorService callers = Executors.newFixedThreadPool(config.threads);
    CountDownLatch ready = new CountDownLatch(config.threads);
    CountDownLatch start = new CountDownLatch(1);
    List<Future<Void>> futures = new ArrayList<>();
    Exception taskFailure = null;

    try {
      for (int i = 0; i < config.threads; i++) {
        futures.add(
            callers.submit(
                createTask(
                    pool,
                    statement,
                    config.invocationsPerThread,
                    ready,
                    start,
                    config.timeoutSeconds)));
      }

      if (!ready.await(config.timeoutSeconds, TimeUnit.SECONDS)) {
        throw new IllegalStateException("benchmark callers failed to get ready in time");
      }

      long beginNs = System.nanoTime();
      start.countDown();
      for (Future<Void> future : futures) {
        try {
          future.get();
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
      long elapsedNs = System.nanoTime() - beginNs;

      long totalInvocations = (long) config.threads * config.invocationsPerThread;
      double elapsedMs = elapsedNs / 1_000_000.0d;
      double throughput = totalInvocations * 1_000.0d / elapsedMs;
      return new BenchmarkMetrics(totalInvocations, elapsedMs, throughput);
    } finally {
      callers.shutdown();
      try {
        if (!callers.awaitTermination(config.timeoutSeconds, TimeUnit.SECONDS)) {
          callers.shutdownNow();
          callers.awaitTermination(Math.max(1, config.timeoutSeconds), TimeUnit.SECONDS);
        }
      } catch (InterruptedException e) {
        callers.shutdownNow();
        Thread.currentThread().interrupt();
      }
      pool.close();
    }
  }

  private Callable<Void> createTask(
      final SessionPool pool,
      final String statement,
      final int invocationsPerThread,
      final CountDownLatch ready,
      final CountDownLatch start,
      final int timeoutSeconds) {
    return () -> {
      ready.countDown();
      if (!start.await(timeoutSeconds, TimeUnit.SECONDS)) {
        throw new IllegalStateException("benchmark start signal timed out");
      }
      for (int i = 0; i < invocationsPerThread; i++) {
        if (Thread.currentThread().isInterrupted()) {
          throw new InterruptedException("benchmark caller interrupted");
        }
        executeSqlAndCheck(pool, statement);
      }
      return null;
    };
  }

  private String buildStatement(String functionName, int rows, int loopsPerInvocation) {
    return String.format(
        Locale.ROOT,
        "SELECT %s(*, %d) FROM %s WHERE key < %d;",
        functionName,
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
      safeExecuteSql(session, String.format(DROP_SQL_TEMPLATE, PURE_FUNCTION_NAME));
      safeExecuteSql(session, String.format(DROP_SQL_TEMPLATE, NUMPY_FUNCTION_NAME));
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

  private void appendCsv(Path outputCsv, List<BenchmarkRecord> records) throws IOException {
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
      lines.add(CSV_HEADER);
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
    private final String mode;
    private final int round;
    private final int threads;
    private final int rows;
    private final int cols;
    private final int loopsPerInvocation;
    private final int invocationsPerThread;
    private final int warmupRounds;
    private final int timeoutSeconds;
    private final String pythonExec;
    private final String metadataUdfScriptPath;
    private final String purePythonUdfScriptPath;
    private final String numpyUdfScriptPath;
    private final Path outputCsv;

    private BenchmarkConfig(
        String host,
        int port,
        String username,
        String password,
        String mode,
        int round,
        int threads,
        int rows,
        int cols,
        int loopsPerInvocation,
        int invocationsPerThread,
        int warmupRounds,
        int timeoutSeconds,
        String pythonExec,
        String metadataUdfScriptPath,
        String purePythonUdfScriptPath,
        String numpyUdfScriptPath,
        Path outputCsv) {
      this.host = host;
      this.port = port;
      this.username = username;
      this.password = password;
      this.mode = mode;
      this.round = round;
      this.threads = threads;
      this.rows = rows;
      this.cols = cols;
      this.loopsPerInvocation = loopsPerInvocation;
      this.invocationsPerThread = invocationsPerThread;
      this.warmupRounds = warmupRounds;
      this.timeoutSeconds = timeoutSeconds;
      this.pythonExec = pythonExec;
      this.metadataUdfScriptPath = metadataUdfScriptPath;
      this.purePythonUdfScriptPath = purePythonUdfScriptPath;
      this.numpyUdfScriptPath = numpyUdfScriptPath;
      this.outputCsv = outputCsv;
    }

    private static BenchmarkConfig fromArgs(String[] args) {
      Map<String, String> values = parseArgs(args);
      String host = values.getOrDefault("host", "127.0.0.1");
      int port = Integer.parseInt(values.getOrDefault("port", "6888"));
      String username = values.getOrDefault("user", "root");
      String password = values.getOrDefault("password", "root");
      String mode = values.getOrDefault("mode", "unknown");
      int round = Integer.parseInt(values.getOrDefault("round", "1"));
      int threads = Integer.parseInt(values.getOrDefault("threads", "1"));
      int rows = Integer.parseInt(values.getOrDefault("rows", "10000"));
      int cols = Integer.parseInt(values.getOrDefault("cols", "10"));
      int loopsPerInvocation = Integer.parseInt(values.getOrDefault("loops", "20"));
      int invocationsPerThread =
          Integer.parseInt(values.getOrDefault("invocations-per-thread", "10"));
      int warmupRounds = Integer.parseInt(values.getOrDefault("warmup", "1"));
      int timeoutSeconds =
          Integer.parseInt(
              values.getOrDefault("timeout-seconds", String.valueOf(DEFAULT_TIMEOUT_SECONDS)));
      String pythonExec = values.get("python-exec");
      String udfDir = values.getOrDefault("udf-dir", "benchmark" + File.separator + "udf");
      if (values.containsKey("udf-script")) {
        File compatibilityPath = new File(values.get("udf-script"));
        File parent = compatibilityPath.getAbsoluteFile().getParentFile();
        if (parent != null) {
          udfDir = parent.getAbsolutePath();
        }
      }
      File udfBaseDir = new File(udfDir).getAbsoluteFile();
      Path outputCsv =
          Paths.get(values.getOrDefault("output-csv", "benchmark/results/raw-results.csv"));
      return new BenchmarkConfig(
          host,
          port,
          username,
          password,
          mode,
          round,
          threads,
          rows,
          cols,
          loopsPerInvocation,
          invocationsPerThread,
          warmupRounds,
          timeoutSeconds,
          pythonExec,
          new File(udfBaseDir, META_SCRIPT_NAME).getAbsolutePath(),
          new File(udfBaseDir, PURE_SCRIPT_NAME).getAbsolutePath(),
          new File(udfBaseDir, NUMPY_SCRIPT_NAME).getAbsolutePath(),
          outputCsv.toAbsolutePath());
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

  private static class Workload {
    private final String name;
    private final String functionName;

    private Workload(String name, String functionName) {
      this.name = name;
      this.functionName = functionName;
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

  private static class BenchmarkMetrics {
    private final long totalInvocations;
    private final double elapsedMs;
    private final double throughput;

    private BenchmarkMetrics(long totalInvocations, double elapsedMs, double throughput) {
      this.totalInvocations = totalInvocations;
      this.elapsedMs = elapsedMs;
      this.throughput = throughput;
    }
  }

  private static class BenchmarkRecord {
    private final String mode;
    private final int round;
    private final String workload;
    private final int threads;
    private final int rows;
    private final int cols;
    private final int loopsPerInvocation;
    private final int invocationsPerThread;
    private final long totalInvocations;
    private final double elapsedMs;
    private final double throughput;
    private final String pythonVersion;
    private final String gilEnabled;
    private final String status;
    private final String error;

    private BenchmarkRecord(
        String mode,
        int round,
        String workload,
        int threads,
        int rows,
        int cols,
        int loopsPerInvocation,
        int invocationsPerThread,
        long totalInvocations,
        double elapsedMs,
        double throughput,
        String pythonVersion,
        String gilEnabled,
        String status,
        String error) {
      this.mode = mode;
      this.round = round;
      this.workload = workload;
      this.threads = threads;
      this.rows = rows;
      this.cols = cols;
      this.loopsPerInvocation = loopsPerInvocation;
      this.invocationsPerThread = invocationsPerThread;
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
        String workload,
        BenchmarkMetrics metrics) {
      return new BenchmarkRecord(
          config.mode,
          config.round,
          workload,
          config.threads,
          config.rows,
          config.cols,
          config.loopsPerInvocation,
          config.invocationsPerThread,
          metrics.totalInvocations,
          metrics.elapsedMs,
          metrics.throughput,
          pythonMode.pythonVersion,
          pythonMode.gilEnabled,
          "ok",
          "");
    }

    private static BenchmarkRecord failure(
        BenchmarkConfig config, PythonModeInfo pythonMode, String workload, Exception e) {
      return new BenchmarkRecord(
          config.mode,
          config.round,
          workload,
          config.threads,
          config.rows,
          config.cols,
          config.loopsPerInvocation,
          config.invocationsPerThread,
          0,
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
          csvEscape(mode),
          String.valueOf(round),
          csvEscape(workload),
          String.valueOf(threads),
          String.valueOf(rows),
          String.valueOf(cols),
          String.valueOf(loopsPerInvocation),
          String.valueOf(invocationsPerThread),
          String.valueOf(totalInvocations),
          formatDouble(elapsedMs),
          formatDouble(throughput),
          csvEscape(pythonVersion),
          csvEscape(gilEnabled),
          csvEscape(status),
          csvEscape(error));
    }
  }
}
