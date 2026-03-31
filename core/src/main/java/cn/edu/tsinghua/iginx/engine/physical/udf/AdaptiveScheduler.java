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

import cn.edu.tsinghua.iginx.resource.system.SystemMetricsService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodically monitors system metrics and adjusts the UDF thread pool size. Uses EWMA to smooth
 * metrics and a cooldown mechanism to prevent oscillation.
 */
public class AdaptiveScheduler {

  private static final Logger LOGGER = LoggerFactory.getLogger(AdaptiveScheduler.class);

  private static final double EWMA_ALPHA = 0.3;
  private static final double ACTIVE_RATIO_EXPAND_THRESHOLD = 0.8;

  private final AdaptiveUDFThreadPoolExecutor pool;
  private final SystemMetricsService metrics;

  private final int expandThreshold;
  private final int shrinkThreshold;
  private final double cpuHighThreshold;
  private final double cpuLowThreshold;
  private final long cooldownMs;
  private final long baseIntervalMs;

  private final ScheduledExecutorService scheduler;
  private ScheduledFuture<?> scheduledTask;

  private volatile long lastAdjustTime = 0;
  private volatile long consecutiveIdleCycles = 0;
  private double ewmaCpuUsage = 0.0;
  private double ewmaQueueSize = 0.0;
  private long currentIntervalMs;

  public AdaptiveScheduler(
      AdaptiveUDFThreadPoolExecutor pool,
      SystemMetricsService metrics,
      int expandThreshold,
      int shrinkThreshold,
      double cpuHighThreshold,
      double cpuLowThreshold,
      long scheduleIntervalMs,
      long cooldownMs) {
    this.pool = pool;
    this.metrics = metrics;
    this.expandThreshold = expandThreshold;
    this.shrinkThreshold = shrinkThreshold;
    this.cpuHighThreshold = cpuHighThreshold;
    this.cpuLowThreshold = cpuLowThreshold;
    this.baseIntervalMs = scheduleIntervalMs;
    this.cooldownMs = cooldownMs;
    this.currentIntervalMs = scheduleIntervalMs;
    this.scheduler =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "udf-adaptive-scheduler");
              t.setDaemon(true);
              return t;
            });
  }

  public void start() {
    scheduleNext(baseIntervalMs);
    LOGGER.info(
        "AdaptiveScheduler started: interval={}ms, cooldown={}ms, expand>{}, shrink<{}, cpuHigh={}, cpuLow={}",
        baseIntervalMs,
        cooldownMs,
        expandThreshold,
        shrinkThreshold,
        cpuHighThreshold,
        cpuLowThreshold);
  }

  public void stop() {
    if (scheduledTask != null) {
      scheduledTask.cancel(false);
    }
    scheduler.shutdown();
    LOGGER.info("AdaptiveScheduler stopped");
  }

  private void scheduleNext(long delayMs) {
    this.currentIntervalMs = delayMs;
    scheduledTask = scheduler.schedule(this::adjust, delayMs, TimeUnit.MILLISECONDS);
  }

  void adjust() {
    try {
      doAdjust();
    } catch (Exception e) {
      LOGGER.warn("Error during adaptive adjustment", e);
    } finally {
      scheduleNext(currentIntervalMs);
    }
  }

  private void doAdjust() {
    double rawCpu = metrics.getRecentCpuUsage();
    int rawQueueSize = pool.getQueue().size();
    int activeCount = pool.getActiveCount();
    int currentSize = pool.getCorePoolSize();

    ewmaCpuUsage = EWMA_ALPHA * rawCpu + (1 - EWMA_ALPHA) * ewmaCpuUsage;
    ewmaQueueSize = EWMA_ALPHA * rawQueueSize + (1 - EWMA_ALPHA) * ewmaQueueSize;

    if (inCooldown()) {
      return;
    }

    double activeRatio = currentSize > 0 ? (double) activeCount / currentSize : 0;

    if (shouldExpand(ewmaQueueSize, ewmaCpuUsage, activeRatio, currentSize)) {
      int newSize = Math.min(currentSize * 2, pool.getMaxPoolSize());
      if (newSize > currentSize) {
        pool.resizePool(newSize);
        markAdjusted();
        consecutiveIdleCycles = 0;
        currentIntervalMs = Math.max(baseIntervalMs / 2, 500);
        LOGGER.debug(
            "EXPAND: cpu={}, queue={}, active={}/{}, newSize={}",
            String.format("%.2f", ewmaCpuUsage),
            String.format("%.1f", ewmaQueueSize),
            activeCount,
            currentSize,
            newSize);
      }
    } else if (shouldShrink(ewmaQueueSize, ewmaCpuUsage, activeRatio, currentSize)) {
      consecutiveIdleCycles++;
      // require sustained idle before shrinking (3+ cycles)
      if (consecutiveIdleCycles >= 3) {
        int newSize = Math.max(currentSize / 2, pool.getMinPoolSize());
        if (newSize < currentSize) {
          pool.resizePool(newSize);
          markAdjusted();
          consecutiveIdleCycles = 0;
          LOGGER.debug(
              "SHRINK: cpu={}, queue={}, active={}/{}, newSize={}",
              String.format("%.2f", ewmaCpuUsage),
              String.format("%.1f", ewmaQueueSize),
              activeCount,
              currentSize,
              newSize);
        }
      }
      currentIntervalMs = Math.min(baseIntervalMs * 2, 10_000);
    } else {
      consecutiveIdleCycles = 0;
      currentIntervalMs = baseIntervalMs;
    }
  }

  private boolean shouldExpand(
      double queueSize, double cpuUsage, double activeRatio, int currentSize) {
    return queueSize > expandThreshold
        && cpuUsage < cpuHighThreshold
        && activeRatio >= ACTIVE_RATIO_EXPAND_THRESHOLD
        && currentSize < pool.getMaxPoolSize();
  }

  private boolean shouldShrink(
      double queueSize, double cpuUsage, double activeRatio, int currentSize) {
    return queueSize <= shrinkThreshold
        && cpuUsage < cpuLowThreshold
        && activeRatio < 0.3
        && currentSize > pool.getMinPoolSize();
  }

  private boolean inCooldown() {
    return System.currentTimeMillis() - lastAdjustTime < cooldownMs;
  }

  private void markAdjusted() {
    lastAdjustTime = System.currentTimeMillis();
  }

  // Visible for testing
  double getEwmaCpuUsage() {
    return ewmaCpuUsage;
  }

  double getEwmaQueueSize() {
    return ewmaQueueSize;
  }

  long getCurrentIntervalMs() {
    return currentIntervalMs;
  }
}
