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
package cn.edu.tsinghua.iginx.statistics;

import cn.edu.tsinghua.iginx.engine.shared.Result;
import cn.edu.tsinghua.iginx.engine.shared.processor.PostExecuteProcessor;
import cn.edu.tsinghua.iginx.engine.shared.processor.PreExecuteProcessor;
import cn.edu.tsinghua.iginx.sql.statement.InsertStatement;
import cn.edu.tsinghua.iginx.sql.statement.Statement;
import cn.edu.tsinghua.iginx.sql.statement.StatementType;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecuteStatisticsCollector extends AbstractStageStatisticsCollector
    implements IExecuteStatisticsCollector {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExecuteStatisticsCollector.class);
  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private final Map<StatementType, Pair<Long, Long>> detailInfos = new HashMap<>();
  private long count = 0;
  private long span = 0;
  private long queryPoints = 0;
  private long insertPoints = 0;

  @Override
  protected String getStageName() {
    return "ExecuteStage";
  }

  @Override
  protected void processStatistics(Statistics statistics) {
    lock.writeLock().lock();
    count += 1;
    span += statistics.getEndTime() - statistics.getStartTime();

    Statement statement = statistics.getContext().getStatement();
    Pair<Long, Long> detailInfo =
        detailInfos.computeIfAbsent(statement.getType(), e -> new Pair<>(0L, 0L));
    detailInfo.k += 1;
    detailInfo.v += statistics.getEndTime() - statistics.getStartTime();
    if (statement.getType() == StatementType.INSERT) {
      InsertStatement insertStatement = (InsertStatement) statement;
      insertPoints += (long) insertStatement.getKeys().size() * insertStatement.getPaths().size();
    }
    if (statement.getType() == StatementType.SELECT) {
      Result result = statistics.getContext().getResult();
      queryPoints += (long) result.getBitmapList().size() * result.getPaths().size();
    }
    lock.writeLock().unlock();
  }

  @Override
  public void broadcastStatistics() {
    lock.readLock().lock();
    LOGGER.info("Execute Stage Statistics Info: ");
    LOGGER.info("\tcount: {}, span: {}μs", count, span);
    if (count != 0) {
      LOGGER.info("\taverage-span: {}μs", (1.0 * span) / count);
    }
    for (Map.Entry<StatementType, Pair<Long, Long>> entry : detailInfos.entrySet()) {
      LOGGER.info(
          "\t\tFor Request: "
              + entry.getKey()
              + ", count: "
              + entry.getValue().k
              + ", span: "
              + entry.getValue().v
              + "μs");
    }
    LOGGER.info("\ttotal insert points: {}", insertPoints);
    LOGGER.info("\ttotal query points: {}", queryPoints);
    lock.readLock().unlock();
  }

  @Override
  public PreExecuteProcessor getPreExecuteProcessor() {
    return before::apply;
  }

  @Override
  public PostExecuteProcessor getPostExecuteProcessor() {
    return after::apply;
  }
}
