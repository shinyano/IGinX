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
package cn.edu.tsinghua.iginx.physical.optimizer.naive.initializer;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.join.JoinOption;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.binary.BinaryExecutorFactory;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.binary.stateful.HashJoinExecutor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.binary.stateful.StatefulBinaryExecutor;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.engine.shared.operator.SingleJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.physical.optimizer.naive.util.HashJoinUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class SingleJoinInfoGenerator implements BinaryExecutorFactory<StatefulBinaryExecutor> {

  private final SingleJoin operator;

  public SingleJoinInfoGenerator(SingleJoin operator) {
    this.operator = Objects.requireNonNull(operator);
  }

  @Override
  public StatefulBinaryExecutor initialize(
      ExecutorContext context, BatchSchema leftSchema, BatchSchema rightSchema)
      throws ComputeException {
    switch (operator.getJoinAlgType()) {
      case HashJoin:
        return initializeHashJoin(context, leftSchema, rightSchema);
      default:
        throw new IllegalStateException(
            "JoinAlgType is not supported: " + operator.getJoinAlgType());
    }
  }

  public HashJoinExecutor initializeHashJoin(
      ExecutorContext context, BatchSchema leftSchema, BatchSchema rightSchema)
      throws ComputeException {

    List<Filter> subFilters = new ArrayList<>();
    if (operator.getFilter() != null) {
      subFilters.add(operator.getFilter());
    }
    for (String extraPrefix : operator.getExtraJoinPrefix()) {
      subFilters.add(new PathFilter(extraPrefix, Op.E, extraPrefix));
    }

    return HashJoinUtils.constructHashJoin(
        context,
        leftSchema,
        rightSchema,
        operator.getPrefixA(),
        operator.getPrefixB(),
        new AndFilter(subFilters),
        Collections.emptySet(),
        JoinOption.SINGLE,
        "&mark",
        false,
        true,
        false);
  }
}
