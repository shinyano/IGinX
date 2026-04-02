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
package cn.edu.tsinghua.iginx.engine.physical.udf;

import cn.edu.tsinghua.iginx.engine.shared.function.manager.ThreadInterpreterManager;
import java.io.ByteArrayOutputStream;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.Assume;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pemja.core.PythonInterpreter;
import pemja.core.PythonInterpreterConfig;

public class PythonConcurrencyBenchmarkTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PythonConcurrencyBenchmarkTest.class);

  private static final int[] THREAD_COUNTS = new int[] {1, 2, 4, 8};
  private static final String PURE_PYTHON = "pure_python";
  private static final String NUMPY_VECTORIZED = "numpy_vectorized";
  private static final String BENCHMARK_OBJECT = "benchmark_driver";
  private static final String LOAD_BENCHMARK_SCRIPT =
      "import sys\n"
          + "if '"
          + BENCHMARK_OBJECT
          + "' not in globals():\n"
          + "    class BenchmarkDriver(object):\n"
          + "        def pure_python(self, rows, cols, loops):\n"
          + "            total = 0.0\n"
          + "            for _ in range(int(loops)):\n"
          + "                for i in range(int(rows)):\n"
          + "                    base = (i + 1) * 0.5\n"
          + "                    for j in range(int(cols)):\n"
          + "                        value = base + j * 0.125\n"
          + "                        total += value * value\n"
          + "            return total\n"
          + "\n"
          + "        def numpy_vectorized(self, rows, cols, loops):\n"
          + "            import numpy as np\n"
          + "            arr = np.arange(int(rows) * int(cols), dtype=np.float64).reshape(int(rows), int(cols))\n"
          + "            total = 0.0\n"
          + "            for _ in range(int(loops)):\n"
          + "                total += float(np.sum(arr * arr))\n"
          + "            return total\n"
          + "\n"
          + "        def metadata(self):\n"
          + "            version = ' '.join(sys.version.split())\n"
          + "            gil = getattr(sys, '_is_gil_enabled', lambda: 'unknown')()\n"
          + "            return {'python_version': version, 'gil_enabled': gil}\n"
          + "\n"
          + "    "
          + BENCHMARK_OBJECT
          + " = BenchmarkDriver()\n";

  @Test
  public void benchmarkPythonConcurrencyScaling() throws Exception {
    Assume.assumeTrue(
        "Python benchmark is disabled. Pass -Diginx.runBenchmark=true to enable.",
        Boolean.getBoolean("iginx.runBenchmark"));

    BenchmarkConfig config = BenchmarkConfig.fromSystemProperties();
    PythonProbeResult runtime = probePythonRuntime(config.pythonExec);
    Assume.assumeTrue(
        "Pemja failed to bootstrap with " + config.pythonExec + ", skip benchmark.",
        canBootstrapPemja(config.pythonExec));

    List<String> workloads = new ArrayList<>();
    workloads.add(PURE_PYTHON);
    if (probePythonImports(config.pythonExec, "import numpy")) {
      workloads.add(NUMPY_VECTORIZED);
    } else {
      LOGGER.warn("Skip numpy benchmark because numpy import failed for {}", config.pythonExec);
    }

    List<BenchmarkRecord> records = new ArrayList<>();
    Map<String, Double> baselineThroughput = new LinkedHashMap<>();
    for (String workload : workloads) {
      for (int threads : THREAD_COUNTS) {
        BenchmarkRecord record = runBenchmark(workload, threads, config, runtime);
        Double baseline = baselineThroughput.get(workload);
        if (baseline == null) {
          baseline = record.throughput;
          baselineThroughput.put(workload, baseline);
        }
        record.speedup = baseline <= 0 ? 0.0d : record.throughput / baseline;
        records.add(record);
        LOGGER.info(
            "Core benchmark result: workload={}, threads={}, throughput={} ops/s, speedup={}",
            workload,
            threads,
            formatDouble(record.throughput),
            formatDouble(record.speedup));
      }
    }

    writeCsv(config.outputCsv, records);
    LOGGER.info("Core benchmark csv written to {}", config.outputCsv.toAbsolutePath());
  }

  private BenchmarkRecord runBenchmark(
      String workload, int threads, BenchmarkConfig config, PythonProbeResult runtime)
      throws Exception {
    PythonInterpreterConfig interpreterConfig =
        PythonInterpreterConfig.newBuilder().setPythonExec(config.pythonExec).build();
    AdaptiveUDFThreadPoolExecutor pool =
        new AdaptiveUDFThreadPoolExecutor(threads, threads, threads, 30, interpreterConfig);
    AdaptiveUDFExecutor executor = AdaptiveUDFExecutor.createForTests(pool);

    try {
      warmUp(workload, threads, executor, config);

      ExecutorService callers = Executors.newFixedThreadPool(threads);
      CountDownLatch ready = new CountDownLatch(threads);
      CountDownLatch start = new CountDownLatch(1);
      List<Future<Double>> futures = new ArrayList<>();
      for (int i = 0; i < threads; i++) {
        futures.add(callers.submit(createCallerTask(workload, executor, config, ready, start)));
      }

      ready.await(30, TimeUnit.SECONDS);
      long beginNs = System.nanoTime();
      start.countDown();
      double checksum = 0.0d;
      for (Future<Double> future : futures) {
        checksum += future.get();
      }
      long elapsedNs = System.nanoTime() - beginNs;
      callers.shutdownNow();

      long totalInvocations = (long) threads * config.invocationsPerThread;
      double elapsedMs = elapsedNs / 1_000_000.0d;
      double throughput = totalInvocations * 1_000.0d / elapsedMs;
      return new BenchmarkRecord(
          "core",
          workload,
          threads,
          config.rows,
          config.cols,
          config.loopsPerInvocation,
          config.invocationsPerThread,
          totalInvocations,
          elapsedMs,
          throughput,
          runtime.pythonVersion,
          runtime.gilEnabled,
          checksum);
    } finally {
      executor.shutdown();
      pool.shutdownNow();
      pool.awaitTermination(30, TimeUnit.SECONDS);
    }
  }

  private Callable<Double> createCallerTask(
      final String workload,
      final AdaptiveUDFExecutor executor,
      final BenchmarkConfig config,
      final CountDownLatch ready,
      final CountDownLatch start) {
    return () -> {
      ready.countDown();
      start.await();
      double checksum = 0.0d;
      for (int i = 0; i < config.invocationsPerThread; i++) {
        checksum +=
            executeWorkload(
                executor, workload, config.rows, config.cols, config.loopsPerInvocation);
      }
      return checksum;
    };
  }

  private void warmUp(
      String workload, int threads, AdaptiveUDFExecutor executor, BenchmarkConfig config)
      throws Exception {
    for (int i = 0; i < threads; i++) {
      executeWorkload(
          executor,
          workload,
          Math.min(config.rows, 64),
          config.cols,
          Math.max(1, Math.min(config.loopsPerInvocation, 2)));
    }
  }

  private double executeWorkload(
      AdaptiveUDFExecutor executor, String workload, int rows, int cols, int loops)
      throws Exception {
    Number result =
        executor.submitAndGet(
            () ->
                ThreadInterpreterManager.executeWithInterpreterAndReturn(
                    interpreter -> {
                      interpreter.exec(LOAD_BENCHMARK_SCRIPT);
                      return (Number)
                          interpreter.invokeMethod(BENCHMARK_OBJECT, workload, rows, cols, loops);
                    }));
    return result.doubleValue();
  }

  private static PythonProbeResult probePythonRuntime(String pythonExec) throws Exception {
    ProcessResult result =
        runCommand(
            pythonExec,
            Arrays.asList(
                "-c",
                "import sys; print(' '.join(sys.version.split()));"
                    + " print(getattr(sys, '_is_gil_enabled', lambda: 'unknown')())"));
    if (result.exitCode != 0) {
      throw new IllegalStateException("Failed to probe python runtime: " + result.stderr);
    }
    String[] lines = result.stdout.split("\\R");
    String version = lines.length > 0 ? lines[0].trim() : "unknown";
    String gilEnabled = lines.length > 1 ? lines[1].trim() : "unknown";
    return new PythonProbeResult(version, gilEnabled);
  }

  private static boolean probePythonImports(String pythonExec, String snippet) throws Exception {
    ProcessResult result = runCommand(pythonExec, Arrays.asList("-c", snippet));
    return result.exitCode == 0;
  }

  private static boolean canBootstrapPemja(String pythonExec) {
    PythonInterpreterConfig interpreterConfig =
        PythonInterpreterConfig.newBuilder().setPythonExec(pythonExec).build();
    try (PythonInterpreter interpreter = new PythonInterpreter(interpreterConfig)) {
      interpreter.exec("bootstrap_ok = True");
      return true;
    } catch (Throwable t) {
      LOGGER.warn("Pemja bootstrap failed for {}", pythonExec, t);
      return false;
    }
  }

  private static ProcessResult runCommand(String executable, List<String> args) throws Exception {
    List<String> command = new ArrayList<>();
    command.add(executable);
    command.addAll(args);
    Process process = new ProcessBuilder(command).start();
    String stdout = readStream(process.getInputStream());
    String stderr = readStream(process.getErrorStream());
    int exitCode = process.waitFor();
    return new ProcessResult(exitCode, stdout, stderr);
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
        "scope,workload,threads,rows,cols,loopsPerInvocation,invocationsPerThread,totalInvocations,elapsedMs,throughput,speedup,pythonVersion,gilEnabled,checksum");
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
    private final String pythonExec;
    private final int rows;
    private final int cols;
    private final int loopsPerInvocation;
    private final int invocationsPerThread;
    private final Path outputCsv;

    private BenchmarkConfig(
        String pythonExec,
        int rows,
        int cols,
        int loopsPerInvocation,
        int invocationsPerThread,
        Path outputCsv) {
      this.pythonExec = pythonExec;
      this.rows = rows;
      this.cols = cols;
      this.loopsPerInvocation = loopsPerInvocation;
      this.invocationsPerThread = invocationsPerThread;
      this.outputCsv = outputCsv;
    }

    private static BenchmarkConfig fromSystemProperties() {
      String pythonExec =
          System.getProperty(
              "iginx.benchmark.python", System.getenv().getOrDefault("pythonCMD", "python"));
      int rows = Integer.getInteger("iginx.benchmark.rows", 2000);
      int cols = Integer.getInteger("iginx.benchmark.cols", 10);
      int loopsPerInvocation = Integer.getInteger("iginx.benchmark.loops", 200);
      int invocationsPerThread = Integer.getInteger("iginx.benchmark.invocationsPerThread", 10);
      Path outputCsv =
          Paths.get(
              System.getProperty(
                  "iginx.benchmark.output",
                  "target/benchmark-results/python-udf-throughput-core.csv"));
      return new BenchmarkConfig(
          pythonExec, rows, cols, loopsPerInvocation, invocationsPerThread, outputCsv);
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
    private final double checksum;
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
        String gilEnabled,
        double checksum) {
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
      this.checksum = checksum;
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
          csvEscape(gilEnabled),
          formatDouble(checksum));
    }
  }

  private static class PythonProbeResult {
    private final String pythonVersion;
    private final String gilEnabled;

    private PythonProbeResult(String pythonVersion, String gilEnabled) {
      this.pythonVersion = pythonVersion;
      this.gilEnabled = gilEnabled;
    }
  }

  private static class ProcessResult {
    private final int exitCode;
    private final String stdout;
    private final String stderr;

    private ProcessResult(int exitCode, String stdout, String stderr) {
      this.exitCode = exitCode;
      this.stdout = stdout;
      this.stderr = stderr;
    }
  }
}
