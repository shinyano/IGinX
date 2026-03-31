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

import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import org.junit.Test;

public class OutputSchemaTest {

  @Test
  public void testStrictEqualsIdentical() {
    OutputSchema a =
        new OutputSchema(
            Arrays.asList("col1", "col2"), Arrays.asList(DataType.LONG, DataType.DOUBLE), false);
    OutputSchema b =
        new OutputSchema(
            Arrays.asList("col1", "col2"), Arrays.asList(DataType.LONG, DataType.DOUBLE), false);
    assertTrue(a.strictEquals(b));
    assertTrue(b.strictEquals(a));
  }

  @Test
  public void testStrictEqualsDifferentNames() {
    OutputSchema a =
        new OutputSchema(
            Arrays.asList("col1", "col2"), Arrays.asList(DataType.LONG, DataType.DOUBLE), false);
    OutputSchema b =
        new OutputSchema(
            Arrays.asList("col1", "col3"), Arrays.asList(DataType.LONG, DataType.DOUBLE), false);
    assertFalse(a.strictEquals(b));
  }

  @Test
  public void testStrictEqualsDifferentTypes() {
    OutputSchema a =
        new OutputSchema(
            Arrays.asList("col1", "col2"), Arrays.asList(DataType.LONG, DataType.DOUBLE), false);
    OutputSchema b =
        new OutputSchema(
            Arrays.asList("col1", "col2"), Arrays.asList(DataType.INTEGER, DataType.DOUBLE), false);
    assertFalse(a.strictEquals(b));
  }

  @Test
  public void testStrictEqualsDifferentKey() {
    OutputSchema a = new OutputSchema(Arrays.asList("col1"), Arrays.asList(DataType.LONG), true);
    OutputSchema b = new OutputSchema(Arrays.asList("col1"), Arrays.asList(DataType.LONG), false);
    assertFalse(a.strictEquals(b));
  }

  @Test
  public void testStrictEqualsNull() {
    OutputSchema a = new OutputSchema(Arrays.asList("col1"), Arrays.asList(DataType.LONG), false);
    assertFalse(a.strictEquals(null));
  }

  @Test
  public void testGetTypeByName() {
    OutputSchema schema =
        new OutputSchema(
            Arrays.asList("a", "b", "c"),
            Arrays.asList(DataType.LONG, DataType.DOUBLE, DataType.BINARY),
            false);
    assertEquals(DataType.LONG, schema.getTypeByName("a"));
    assertEquals(DataType.DOUBLE, schema.getTypeByName("b"));
    assertEquals(DataType.BINARY, schema.getTypeByName("c"));
    assertNull(schema.getTypeByName("nonexistent"));
  }

  @Test
  public void testGetColumnNameSet() {
    OutputSchema schema =
        new OutputSchema(
            Arrays.asList("x", "y"), Arrays.asList(DataType.LONG, DataType.LONG), false);
    Set<String> set = schema.getColumnNameSet();
    assertEquals(2, set.size());
    assertTrue(set.contains("x"));
    assertTrue(set.contains("y"));
  }

  @Test
  public void testImmutability() {
    OutputSchema schema =
        new OutputSchema(Arrays.asList("col1"), Arrays.asList(DataType.LONG), false);
    try {
      schema.getColumnNames().add("col2");
      fail("Should not allow mutation of column names");
    } catch (UnsupportedOperationException e) {
      // expected
    }
  }

  @Test
  public void testToString() {
    OutputSchema schema =
        new OutputSchema(
            Collections.singletonList("s1"), Collections.singletonList(DataType.LONG), true);
    String s = schema.toString();
    assertTrue(s.contains("s1"));
    assertTrue(s.contains("LONG"));
    assertTrue(s.contains("hasKey=true"));
  }
}
