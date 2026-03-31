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

import cn.edu.tsinghua.iginx.engine.shared.function.manager.FunctionManager;
import cn.edu.tsinghua.iginx.engine.shared.function.manager.ThreadInterpreterManager;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pemja.core.PythonInterpreterConfig;

/**
 * Thread pool dedicated to Python UDF execution with dynamic resizing capability. Unlike the
 * fixed-size AbstractTaskThreadPoolExecutor, this pool supports runtime adjustment of core/max pool
 * size driven by AdaptiveScheduler.
 */
public class AdaptiveUDFThreadPoolExecutor extends ThreadPoolExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(AdaptiveUDFThreadPoolExecutor.class);

  private final PythonInterpreterConfig interpreterConfig;
  private final int minPoolSize;
  private final int maxPoolSize;

  public AdaptiveUDFThreadPoolExecutor(
      int initialSize, int minPoolSize, int maxPoolSize, long keepAliveSeconds) {
    this(initialSize, minPoolSize, maxPoolSize, keepAliveSeconds, null);
  }

  /**
   * @param config if null, uses FunctionManager's default config (normal production path). Pass
   *     non-null to override (useful in tests or when FunctionManager is not available).
   */
  public AdaptiveUDFThreadPoolExecutor(
      int initialSize,
      int minPoolSize,
      int maxPoolSize,
      long keepAliveSeconds,
      PythonInterpreterConfig config) {
    super(
        initialSize,
        maxPoolSize,
        keepAliveSeconds,
        TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(),
        new UDFThreadFactory());
    this.minPoolSize = minPoolSize;
    this.maxPoolSize = maxPoolSize;
    this.interpreterConfig = config != null ? config : FunctionManager.getInstance().getConfig();
    allowCoreThreadTimeOut(true);
  }

  public int getMinPoolSize() {
    return minPoolSize;
  }

  public int getMaxPoolSize() {
    return maxPoolSize;
  }

  /**
   * Dynamically resize the pool. Values are clamped to [minPoolSize, maxPoolSize]. Thread-safe: the
   * underlying ThreadPoolExecutor handles concurrent setCorePoolSize/setMaximumPoolSize correctly.
   */
  public void resizePool(int newSize) {
    newSize = Math.max(newSize, minPoolSize);
    newSize = Math.min(newSize, maxPoolSize);
    int oldSize = getCorePoolSize();
    if (newSize == oldSize) {
      return;
    }
    if (newSize > oldSize) {
      setMaximumPoolSize(newSize);
      setCorePoolSize(newSize);
    } else {
      setCorePoolSize(newSize);
      setMaximumPoolSize(newSize);
    }
    LOGGER.info("UDF thread pool resized: {} -> {}", oldSize, newSize);
  }

  @Override
  protected void beforeExecute(Thread t, Runnable r) {
    super.beforeExecute(t, r);
    ThreadInterpreterManager.setConfig(interpreterConfig);
  }

  @Override
  protected void afterExecute(Runnable r, Throwable t) {
    super.afterExecute(r, t);
    if (t != null) {
      LOGGER.error("UDF task completed with exception", t);
    }
  }

  /**
   * ThreadFactory that wraps the worker Runnable so that when a thread exits (due to timeout or
   * pool shrink), its ThreadLocal PythonInterpreter is properly closed.
   */
  private static class UDFThreadFactory implements ThreadFactory {
    private static final AtomicInteger POOL_NUMBER = new AtomicInteger(1);
    private final AtomicInteger threadNumber = new AtomicInteger(1);
    private final String namePrefix;

    UDFThreadFactory() {
      namePrefix = "udf-pool-" + POOL_NUMBER.getAndIncrement() + "-thread-";
    }

    @Override
    public Thread newThread(Runnable r) {
      Thread thread =
          new Thread(
              () -> {
                try {
                  r.run();
                } finally {
                  if (ThreadInterpreterManager.isInterpreterSet()) {
                    try {
                      ThreadInterpreterManager.getInterpreter().close();
                    } catch (Exception e) {
                      LOGGER.warn("Failed to close PythonInterpreter on thread exit", e);
                    }
                  }
                }
              },
              namePrefix + threadNumber.getAndIncrement());
      thread.setDaemon(true);
      thread.setUncaughtExceptionHandler(
          (t, e) -> LOGGER.error("Uncaught exception in UDF thread: {}", t.getName(), e));
      return thread;
    }
  }
}
