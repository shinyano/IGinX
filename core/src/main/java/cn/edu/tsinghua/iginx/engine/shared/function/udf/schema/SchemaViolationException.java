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

/** UDF 输出模式一致性校验失败时抛出的异常，携带结构化差异信息。 */
public class SchemaViolationException extends Exception {

  private final String callSiteId;
  private final OutputSchema expected;
  private final OutputSchema actual;

  public SchemaViolationException(
      String callSiteId, OutputSchema expected, OutputSchema actual, String detail) {
    super(buildMessage(callSiteId, expected, actual, detail));
    this.callSiteId = callSiteId;
    this.expected = expected;
    this.actual = actual;
  }

  private static String buildMessage(
      String callSiteId, OutputSchema expected, OutputSchema actual, String detail) {
    return String.format(
        "Schema violation at call site [%s]: %s. Expected: %s, Actual: %s",
        callSiteId, detail, expected, actual);
  }

  public String getCallSiteId() {
    return callSiteId;
  }

  public OutputSchema getExpected() {
    return expected;
  }

  public OutputSchema getActual() {
    return actual;
  }
}
