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

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.resource.system.DefaultSystemMetricsService;
import cn.edu.tsinghua.iginx.resource.system.SystemMetricsService;
import java.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Singleton facade for adaptive UDF thread pool execution. Manages the lifecycle of the pool and
 * the adaptive scheduler. When disabled via config, {@link #submitAndGet} runs the task on the
 * caller thread directly.
 */
public class AdaptiveUDFExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(AdaptiveUDFExecutor.class);

  private final AdaptiveUDFThreadPoolExecutor pool;
  private final AdaptiveScheduler scheduler;
  private final SystemMetricsService metricsService;
  private final boolean enabled;

  private AdaptiveUDFExecutor() {
    Config config = ConfigDescriptor.getInstance().getConfig();
    this.enabled = config.isUdfPoolEnabled();

    if (enabled) {
      this.metricsService = new DefaultSystemMetricsService();
      this.metricsService.start();

      this.pool =
          new AdaptiveUDFThreadPoolExecutor(
              config.getUdfPoolInitialThreads(),
              config.getUdfPoolMinThreads(),
              config.getUdfPoolMaxThreads(),
              config.getUdfPoolKeepAliveSeconds());

      this.scheduler =
          new AdaptiveScheduler(
              pool,
              metricsService,
              config.getUdfPoolExpandThreshold(),
              config.getUdfPoolShrinkThreshold(),
              config.getUdfPoolCpuHighThreshold(),
              config.getUdfPoolCpuLowThreshold(),
              config.getUdfPoolScheduleIntervalMs(),
              config.getUdfPoolCooldownMs());
      scheduler.start();

      LOGGER.info(
          "AdaptiveUDFExecutor initialized: initial={}, min={}, max={}, keepAlive={}s",
          config.getUdfPoolInitialThreads(),
          config.getUdfPoolMinThreads(),
          config.getUdfPoolMaxThreads(),
          config.getUdfPoolKeepAliveSeconds());
    } else {
      this.pool = null;
      this.scheduler = null;
      this.metricsService = null;
      LOGGER.info("AdaptiveUDFExecutor disabled, UDF runs on caller thread");
    }
  }

  public static AdaptiveUDFExecutor getInstance() {
    return Holder.INSTANCE;
  }

  /**
   * Submit a UDF task and block until result is available. If the adaptive pool is disabled, the
   * task runs directly on the caller thread.
   */
  public <T> T submitAndGet(Callable<T> task) throws Exception {
    if (!enabled || pool == null) {
      return task.call();
    }
    Future<T> future = pool.submit(task);
    try {
      return future.get();
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof Exception) {
        throw (Exception) cause;
      }
      throw new RuntimeException(cause);
    }
  }

  /**
   * Submit a UDF task with a timeout. If the adaptive pool is disabled, the task runs directly on
   * the caller thread (timeout is ignored in that case).
   */
  public <T> T submitAndGet(Callable<T> task, long timeout, TimeUnit unit) throws Exception {
    if (!enabled || pool == null) {
      return task.call();
    }
    Future<T> future = pool.submit(task);
    try {
      return future.get(timeout, unit);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof Exception) {
        throw (Exception) cause;
      }
      throw new RuntimeException(cause);
    }
  }

  public boolean isEnabled() {
    return enabled;
  }

  /** Returns the current pool size, or 0 if disabled. */
  public int getCurrentPoolSize() {
    return enabled && pool != null ? pool.getCorePoolSize() : 0;
  }

  public void shutdown() {
    if (scheduler != null) {
      scheduler.stop();
    }
    if (pool != null) {
      pool.shutdown();
    }
    if (metricsService != null) {
      metricsService.stop();
    }
    LOGGER.info("AdaptiveUDFExecutor shut down");
  }

  private static class Holder {
    private static final AdaptiveUDFExecutor INSTANCE = new AdaptiveUDFExecutor();
  }
}
