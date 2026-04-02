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
package cn.edu.tsinghua.iginx.integration.func.udf;

import static cn.edu.tsinghua.iginx.integration.controller.Controller.clearAllData;

import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.func.session.InsertAPIType;
import cn.edu.tsinghua.iginx.integration.tool.MultiConnection;
import cn.edu.tsinghua.iginx.integration.tool.SQLExecutor;
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
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UDFThroughputBenchmarkIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(UDFThroughputBenchmarkIT.class);

  private static final int[] THREAD_COUNTS = new int[] {1, 2, 4, 8};
  private static final String PURE_FUNCTION_NAME = "throughput_pure_bench";
  private static final String NUMPY_FUNCTION_NAME = "throughput_numpy_bench";
  private static final String META_FUNCTION_NAME = "throughput_meta_bench";
  private static final String REGISTER_SQL = "CREATE FUNCTION %s \"%s\" FROM \"%s\" IN \"%s\";";
  private static final String SERIES_PREFIX = "bench.d1";
  private static final String SCRIPT_FILE_PATH =
      String.join(
          File.separator,
          System.getProperty("user.dir"),
          "src",
          "test",
          "resources",
          "udf",
          "throughput_benchmark_udsf.py");

  private static Session session;
  private static UDFTestTools tool;
  private static final List<String> tasksToBeRemoved = new ArrayList<>();

  @BeforeClass
  public static void setUp() throws Exception {
    Assume.assumeTrue(
        "E2E benchmark is disabled. Pass -Diginx.runBenchmark=true to enable.",
        Boolean.getBoolean("iginx.runBenchmark"));
    session = new Session("127.0.0.1", 6888, "root", "root");
    session.openSession();
    tool = new UDFTestTools(session);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    try {
      if (tool != null) {
        tool.dropTasks(tasksToBeRemoved);
      }
      if (session != null) {
        clearAllData(session);
      }
    } finally {
      tasksToBeRemoved.clear();
      if (session != null) {
        session.closeSession();
      }
    }
  }

  @Test
  public void benchmarkConcurrentUdfThroughput() throws Exception {
    BenchmarkConfig config = BenchmarkConfig.fromSystemProperties();
    Assume.assumeTrue(
        "Server-side benchmark UDF requires a healthy pandas + iginx_udf runtime.",
        probePythonImports(config.serverPythonProbe, "import pandas; import iginx_udf"));
    boolean numpyHealthy =
        probePythonImports(
            config.serverPythonProbe, "import numpy; import pandas; import iginx_udf");

    Controller.clearData(session);
    registerFunctions(numpyHealthy);
    insertData(config.rows, config.cols);
    Controller.after(session);

    PythonModeInfo pythonMode = fetchPythonMode();
    LOGGER.info(
        "Server-side benchmark python mode: version={}.{}.{} gil_enabled={}",
        pythonMode.major,
        pythonMode.minor,
        pythonMode.micro,
        pythonMode.gilEnabled);

    List<BenchmarkRecord> records = new ArrayList<>();
    Map<String, Double> baselineThroughput = new LinkedHashMap<>();

    List<Workload> workloads = new ArrayList<>();
    workloads.add(new Workload("pure_python", PURE_FUNCTION_NAME));
    if (numpyHealthy) {
      workloads.add(new Workload("numpy_vectorized", NUMPY_FUNCTION_NAME));
    } else {
      LOGGER.warn(
          "Skip numpy E2E benchmark because numpy import failed for {}", config.serverPythonProbe);
    }

    for (Workload workload : workloads) {
      for (int threads : THREAD_COUNTS) {
        BenchmarkRecord record = runBenchmark(workload, threads, config, pythonMode);
        Double baseline = baselineThroughput.get(workload.name);
        if (baseline == null) {
          baseline = record.throughput;
          baselineThroughput.put(workload.name, baseline);
        }
        record.speedup = baseline <= 0 ? 0.0d : record.throughput / baseline;
        records.add(record);
        LOGGER.info(
            "E2E benchmark result: workload={}, threads={}, throughput={} ops/s, speedup={}",
            workload.name,
            threads,
            formatDouble(record.throughput),
            formatDouble(record.speedup));
      }
    }

    writeCsv(config.outputCsv, records);
    LOGGER.info("E2E benchmark csv written to {}", config.outputCsv.toAbsolutePath());
  }

  private void registerFunctions(boolean numpyHealthy) {
    registerFunction(META_FUNCTION_NAME, "BenchmarkMetadata");
    registerFunction(PURE_FUNCTION_NAME, "ComputeIntensiveBenchmark");
    if (numpyHealthy) {
      registerFunction(NUMPY_FUNCTION_NAME, "NumpyBenchmark");
    }
  }

  private void registerFunction(String functionName, String className) {
    tool.executeReg(String.format(REGISTER_SQL, "UDSF", functionName, className, SCRIPT_FILE_PATH));
    if (!tasksToBeRemoved.contains(functionName)) {
      tasksToBeRemoved.add(functionName);
    }
  }

  private void insertData(int rows, int cols) {
    List<String> pathList = new ArrayList<>();
    List<DataType> dataTypeList = new ArrayList<>();
    for (int i = 1; i <= cols; i++) {
      pathList.add(SERIES_PREFIX + ".s" + i);
      dataTypeList.add(DataType.DOUBLE);
    }

    List<Long> keyList = new ArrayList<>();
    List<List<Object>> valuesList = new ArrayList<>();
    for (int row = 0; row < rows; row++) {
      keyList.add((long) row);
      List<Object> rowValues = new ArrayList<>();
      for (int col = 0; col < cols; col++) {
        rowValues.add(row + col / 10.0d);
      }
      valuesList.add(rowValues);
    }

    Controller.writeRowsData(
        session,
        pathList,
        keyList,
        dataTypeList,
        valuesList,
        new ArrayList<>(),
        InsertAPIType.Row,
        true);
  }

  private PythonModeInfo fetchPythonMode() {
    SessionExecuteSqlResult result =
        tool.execute(
            String.format(
                "SELECT %s(*) FROM %s WHERE key < 1;", META_FUNCTION_NAME, SERIES_PREFIX));
    List<Object> values = result.getValues().get(0);
    return new PythonModeInfo(
        ((Number) values.get(0)).intValue(),
        ((Number) values.get(1)).intValue(),
        ((Number) values.get(2)).intValue(),
        ((Number) values.get(3)).intValue());
  }

  private BenchmarkRecord runBenchmark(
      Workload workload, int threads, BenchmarkConfig config, PythonModeInfo pythonMode)
      throws Exception {
    String statement =
        String.format(
            "SELECT %s(*, %d) FROM %s WHERE key < %d;",
            workload.functionName, config.loopsPerInvocation, SERIES_PREFIX, config.rows);
    List<String> statements = new ArrayList<>();
    for (int i = 0; i < threads * config.invocationsPerThread; i++) {
      statements.add(statement);
    }

    SQLExecutor executor =
        new SQLExecutor(
            new MultiConnection(new SessionPool("127.0.0.1", 6888, "root", "root", threads * 2)),
            threads);
    executor.setNeedCompareResult(false);

    try {
      long beginNs = System.nanoTime();
      executor.concurrentExecute(statements);
      long elapsedNs = System.nanoTime() - beginNs;

      long totalInvocations = (long) threads * config.invocationsPerThread;
      double elapsedMs = elapsedNs / 1_000_000.0d;
      double throughput = totalInvocations * 1_000.0d / elapsedMs;
      return new BenchmarkRecord(
          "e2e",
          workload.name,
          threads,
          config.rows,
          config.cols,
          config.loopsPerInvocation,
          config.invocationsPerThread,
          totalInvocations,
          elapsedMs,
          throughput,
          pythonMode.toVersionString(),
          String.valueOf(pythonMode.gilEnabled));
    } finally {
      executor.close();
    }
  }

  private static boolean probePythonImports(String pythonExec, String snippet) throws Exception {
    ProcessResult result = runCommand(pythonExec, Arrays.asList("-c", snippet));
    return result.exitCode == 0;
  }

  private static ProcessResult runCommand(String executable, List<String> args) throws Exception {
    List<String> command = new ArrayList<>();
    command.add(executable);
    command.addAll(args);
    Process process = new ProcessBuilder(command).start();
    readStream(process.getInputStream());
    readStream(process.getErrorStream());
    int exitCode = process.waitFor();
    return new ProcessResult(exitCode);
  }

  private static String readStream(InputStream stream) throws IOException {
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

  private static void writeCsv(Path outputCsv, List<BenchmarkRecord> records) throws IOException {
    Files.createDirectories(outputCsv.getParent());
    List<String> lines = new ArrayList<>();
    lines.add(
        "scope,workload,threads,rows,cols,loopsPerInvocation,invocationsPerThread,totalInvocations,elapsedMs,throughput,speedup,pythonVersion,gilEnabled");
    for (BenchmarkRecord record : records) {
      lines.add(record.toCsvLine());
    }
    Files.write(
        outputCsv,
        lines,
        StandardCharsets.UTF_8,
        StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING,
        StandardOpenOption.WRITE);
  }

  private static String formatDouble(double value) {
    return String.format(Locale.ROOT, "%.4f", value);
  }

  private static String csvEscape(String value) {
    return "\"" + value.replace("\"", "\"\"") + "\"";
  }

  private static class BenchmarkConfig {
    private final int rows;
    private final int cols;
    private final int loopsPerInvocation;
    private final int invocationsPerThread;
    private final String serverPythonProbe;
    private final Path outputCsv;

    private BenchmarkConfig(
        int rows,
        int cols,
        int loopsPerInvocation,
        int invocationsPerThread,
        String serverPythonProbe,
        Path outputCsv) {
      this.rows = rows;
      this.cols = cols;
      this.loopsPerInvocation = loopsPerInvocation;
      this.invocationsPerThread = invocationsPerThread;
      this.serverPythonProbe = serverPythonProbe;
      this.outputCsv = outputCsv;
    }

    private static BenchmarkConfig fromSystemProperties() {
      int rows = Integer.getInteger("iginx.benchmark.rows", 2000);
      int cols = Integer.getInteger("iginx.benchmark.cols", 10);
      int loopsPerInvocation = Integer.getInteger("iginx.benchmark.e2e.loops", 20);
      int invocationsPerThread = Integer.getInteger("iginx.benchmark.invocationsPerThread", 10);
      String serverPythonProbe =
          System.getProperty(
              "iginx.benchmark.serverPython",
              System.getProperty(
                  "iginx.benchmark.python", System.getenv().getOrDefault("pythonCMD", "python")));
      Path outputCsv =
          Paths.get(
              System.getProperty(
                  "iginx.benchmark.e2e.output",
                  "target/benchmark-results/python-udf-throughput-e2e.csv"));
      return new BenchmarkConfig(
          rows, cols, loopsPerInvocation, invocationsPerThread, serverPythonProbe, outputCsv);
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
    private final int gilEnabled;
    private final int major;
    private final int minor;
    private final int micro;

    private PythonModeInfo(int gilEnabled, int major, int minor, int micro) {
      this.gilEnabled = gilEnabled;
      this.major = major;
      this.minor = minor;
      this.micro = micro;
    }

    private String toVersionString() {
      return major + "." + minor + "." + micro;
    }
  }

  private static class BenchmarkRecord {
    private final String scope;
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
    private double speedup;

    private BenchmarkRecord(
        String scope,
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
        String gilEnabled) {
      this.scope = scope;
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
    }

    private String toCsvLine() {
      return String.join(
          ",",
          scope,
          workload,
          String.valueOf(threads),
          String.valueOf(rows),
          String.valueOf(cols),
          String.valueOf(loopsPerInvocation),
          String.valueOf(invocationsPerThread),
          String.valueOf(totalInvocations),
          formatDouble(elapsedMs),
          formatDouble(throughput),
          formatDouble(speedup),
          csvEscape(pythonVersion),
          csvEscape(gilEnabled));
    }
  }

  private static class ProcessResult {
    private final int exitCode;

    private ProcessResult(int exitCode) {
      this.exitCode = exitCode;
    }
  }
}
