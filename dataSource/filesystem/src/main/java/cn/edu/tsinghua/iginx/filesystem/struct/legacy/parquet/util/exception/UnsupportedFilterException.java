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
package cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.util.exception;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;

public class UnsupportedFilterException extends StorageException {
  private final Filter filter;

  public UnsupportedFilterException(Filter filter) {
    super(String.format("unsupported filter %s", filter.toString()));
    this.filter = filter;
  }

  public UnsupportedFilterException(String message, Filter filter) {
    super(message);
    this.filter = filter;
  }

  public Filter getFilter() {
    return filter;
  }
}
