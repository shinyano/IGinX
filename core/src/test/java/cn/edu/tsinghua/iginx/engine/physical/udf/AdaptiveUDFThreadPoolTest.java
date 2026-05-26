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

import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.resource.system.SystemMetricsService;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import pemja.core.PythonInterpreterConfig;

public class AdaptiveUDFThreadPoolTest {

  private AdaptiveUDFThreadPoolExecutor pool;

  private static PythonInterpreterConfig dummyConfig() {
    return PythonInterpreterConfig.newBuilder().setPythonExec("python").build();
  }

  @Before
  public void setUp() {
    pool = new AdaptiveUDFThreadPoolExecutor(4, 2, 16, 2, dummyConfig());
  }

  @After
  public void tearDown() {
    if (pool != null && !pool.isShutdown()) {
      pool.shutdownNow();
    }
  }

  @Test
  public void testInitialPoolSize() {
    assertEquals(4, pool.getCorePoolSize());
    assertEquals(2, pool.getMinPoolSize());
    assertEquals(16, pool.getMaxPoolSize());
  }

  @Test
  public void testResizePoolExpand() {
    pool.resizePool(8);
    assertEquals(8, pool.getCorePoolSize());
  }

  @Test
  public void testResizePoolShrink() {
    pool.resizePool(8);
    assertEquals(8, pool.getCorePoolSize());

    pool.resizePool(3);
    assertEquals(3, pool.getCorePoolSize());
  }

  @Test
  public void testResizePoolClampedToMin() {
    pool.resizePool(1);
    assertEquals(2, pool.getCorePoolSize());
  }

  @Test
  public void testResizePoolClampedToMax() {
    pool.resizePool(100);
    assertEquals(16, pool.getCorePoolSize());
  }

  @Test
  public void testResizePoolNoOpOnSameSize() {
    int before = pool.getCorePoolSize();
    pool.resizePool(before);
    assertEquals(before, pool.getCorePoolSize());
  }

  @Test
  public void testTaskExecution() throws Exception {
    Future<Integer> future = pool.submit(() -> 42);
    assertEquals(Integer.valueOf(42), future.get(5, TimeUnit.SECONDS));
  }

  @Test
  public void testConcurrentTasks() throws Exception {
    int taskCount = 20;
    AtomicInteger counter = new AtomicInteger(0);
    List<Future<?>> futures = new ArrayList<>();

    for (int i = 0; i < taskCount; i++) {
      futures.add(
          pool.submit(
              () -> {
                counter.incrementAndGet();
                return null;
              }));
    }

    for (Future<?> f : futures) {
      f.get(5, TimeUnit.SECONDS);
    }
    assertEquals(taskCount, counter.get());
  }

  @Test
  public void testResizeDuringExecution() throws Exception {
    CountDownLatch started = new CountDownLatch(4);
    CountDownLatch proceed = new CountDownLatch(1);
    List<Future<?>> futures = new ArrayList<>();

    for (int i = 0; i < 4; i++) {
      futures.add(
          pool.submit(
              () -> {
                started.countDown();
                proceed.await();
                return null;
              }));
    }

    assertTrue(started.await(5, TimeUnit.SECONDS));

    pool.resizePool(8);
    assertEquals(8, pool.getCorePoolSize());

    proceed.countDown();
    for (Future<?> f : futures) {
      f.get(5, TimeUnit.SECONDS);
    }
  }

  // --- AdaptiveScheduler tests ---

  @Test
  public void testSchedulerExpand() throws Exception {
    StubMetrics metrics = new StubMetrics(0.5, 0.3);
    AdaptiveScheduler scheduler = new AdaptiveScheduler(pool, metrics, 3, 1, 0.85, 0.3, 100, 0);

    for (int i = 0; i < 10; i++) {
      pool.submit(
          () -> {
            Thread.sleep(5000);
            return null;
          });
    }
    Thread.sleep(100);

    // EWMA needs several iterations to converge past the threshold
    for (int i = 0; i < 5; i++) {
      scheduler.adjust();
    }
    assertTrue(
        "Pool should have expanded from 4, actual=" + pool.getCorePoolSize(),
        pool.getCorePoolSize() > 4);
  }

  @Test
  public void testSchedulerShrinkRequiresSustainedIdle() throws Exception {
    pool.resizePool(8);
    StubMetrics metrics = new StubMetrics(0.1, 0.2);
    AdaptiveScheduler scheduler = new AdaptiveScheduler(pool, metrics, 3, 1, 0.85, 0.3, 100, 0);

    scheduler.adjust();
    assertEquals("Should not shrink after 1 idle cycle", 8, pool.getCorePoolSize());

    scheduler.adjust();
    assertEquals("Should not shrink after 2 idle cycles", 8, pool.getCorePoolSize());

    scheduler.adjust();
    assertTrue("Should shrink after 3+ idle cycles", pool.getCorePoolSize() < 8);
  }

  @Test
  public void testSchedulerCooldown() throws Exception {
    StubMetrics metrics = new StubMetrics(0.5, 0.3);
    long cooldownMs = 5000;
    AdaptiveScheduler scheduler =
        new AdaptiveScheduler(pool, metrics, 3, 1, 0.85, 0.3, 100, cooldownMs);

    for (int i = 0; i < 10; i++) {
      pool.submit(
          () -> {
            Thread.sleep(5000);
            return null;
          });
    }
    Thread.sleep(100);

    // EWMA needs several iterations to converge past the threshold
    for (int i = 0; i < 5; i++) {
      scheduler.adjust();
    }
    int sizeAfterFirst = pool.getCorePoolSize();
    assertTrue("Should have expanded, actual=" + sizeAfterFirst, sizeAfterFirst > 4);

    // subsequent call should be blocked by cooldown
    scheduler.adjust();
    assertEquals("Should not adjust during cooldown", sizeAfterFirst, pool.getCorePoolSize());
  }

  @Test
  public void testSchedulerNoCpuOverload() throws Exception {
    StubMetrics metrics = new StubMetrics(0.95, 0.3);
    AdaptiveScheduler scheduler = new AdaptiveScheduler(pool, metrics, 3, 1, 0.85, 0.3, 100, 0);

    for (int i = 0; i < 10; i++) {
      pool.submit(
          () -> {
            Thread.sleep(2000);
            return null;
          });
    }
    Thread.sleep(50);

    scheduler.adjust();
    assertEquals("Should NOT expand when CPU is overloaded", 4, pool.getCorePoolSize());
  }

  @Test
  public void testEwmaSmoothing() throws Exception {
    StubMetrics metrics = new StubMetrics(0.0, 0.0);
    AdaptiveScheduler scheduler = new AdaptiveScheduler(pool, metrics, 3, 1, 0.85, 0.3, 100, 0);

    metrics.setCpu(1.0);
    scheduler.adjust();
    double ewma1 = scheduler.getEwmaCpuUsage();
    assertTrue("EWMA should be smoothed, not jump to 1.0", ewma1 < 1.0);
    assertTrue("EWMA should reflect some of the spike", ewma1 > 0.0);

    metrics.setCpu(0.0);
    scheduler.adjust();
    double ewma2 = scheduler.getEwmaCpuUsage();
    assertTrue("EWMA should decay towards 0", ewma2 < ewma1);
  }

  /** Simple stub for SystemMetricsService to control CPU/memory values in tests. */
  private static class StubMetrics implements SystemMetricsService {
    private volatile double cpu;
    private volatile double memory;

    StubMetrics(double cpu, double memory) {
      this.cpu = cpu;
      this.memory = memory;
    }

    void setCpu(double cpu) {
      this.cpu = cpu;
    }

    @Override
    public void start() {}

    @Override
    public void stop() {}

    @Override
    public double getRecentCpuUsage() {
      return cpu;
    }

    @Override
    public double getRecentMemoryUsage() {
      return memory;
    }
  }
}
