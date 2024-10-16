/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
<<<<<<<< HEAD:core/src/main/java/cn/edu/tsinghua/iginx/engine/physical/memory/execute/compute/util/ComputingCloseable.java
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util;
========
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.function.expression;
>>>>>>>> 70c4ce540 (binary & unary expression implementation):core/src/main/java/cn/edu/tsinghua/iginx/engine/physical/memory/execute/compute/function/expression/PhysicalExpression.java

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;

public interface ComputingCloseable extends AutoCloseable {

  @Override
  void close() throws ComputeException;
}
