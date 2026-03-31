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

/**
 * 类型升级允许矩阵。
 *
 * <p>IGinX 数据类型：BOOLEAN, INTEGER, LONG, FLOAT, DOUBLE, BINARY。
 *
 * <p>保守策略：仅允许同族内加宽，不允许跨族。
 *
 * <ul>
 *   <li>INTEGER → LONG（有符号整型加宽）
 *   <li>FLOAT → DOUBLE（浮点加宽）
 *   <li>其他类型仅允许恒等
 * </ul>
 */
public class TypeUpcastMatrix {

  private TypeUpcastMatrix() {}

  /** 判断从 src 升级到 target 是否允许。 */
  public static boolean isUpcastAllowed(DataType src, DataType target) {
    if (src == target) {
      return true;
    }
    if (src == DataType.INTEGER && target == DataType.LONG) {
      return true;
    }
    if (src == DataType.FLOAT && target == DataType.DOUBLE) {
      return true;
    }
    return false;
  }
}
