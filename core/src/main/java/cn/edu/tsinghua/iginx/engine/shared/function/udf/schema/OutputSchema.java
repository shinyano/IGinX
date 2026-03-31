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
package cn.edu.tsinghua.iginx.engine.shared.function.udf.schema;

import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.*;

/**
 * UDF 输出模式 σ = (C, T, κ)。
 *
 * <ul>
 *   <li>C — 有序列名序列
 *   <li>T — 与 C 对齐的类型序列
 *   <li>κ — 键语义（true = 有 key，false = 无 key）
 * </ul>
 */
public class OutputSchema {

  private final List<String> columnNames;
  private final List<DataType> columnTypes;
  private final boolean hasKey;

  public OutputSchema(List<String> columnNames, List<DataType> columnTypes, boolean hasKey) {
    this.columnNames = Collections.unmodifiableList(new ArrayList<>(columnNames));
    this.columnTypes = Collections.unmodifiableList(new ArrayList<>(columnTypes));
    this.hasKey = hasKey;
  }

  public List<String> getColumnNames() {
    return columnNames;
  }

  public List<DataType> getColumnTypes() {
    return columnTypes;
  }

  public boolean isHasKey() {
    return hasKey;
  }

  public Set<String> getColumnNameSet() {
    return new LinkedHashSet<>(columnNames);
  }

  public DataType getTypeByName(String name) {
    int idx = columnNames.indexOf(name);
    return idx >= 0 ? columnTypes.get(idx) : null;
  }

  /** 严格相等：列名序列、类型序列、键语义三者完全一致。 */
  public boolean strictEquals(OutputSchema other) {
    if (other == null) return false;
    return this.hasKey == other.hasKey
        && this.columnNames.equals(other.columnNames)
        && this.columnTypes.equals(other.columnTypes);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("OutputSchema{columns=[");
    for (int i = 0; i < columnNames.size(); i++) {
      if (i > 0) sb.append(", ");
      sb.append(columnNames.get(i)).append(':').append(columnTypes.get(i));
    }
    sb.append("], hasKey=").append(hasKey).append('}');
    return sb.toString();
  }
}
