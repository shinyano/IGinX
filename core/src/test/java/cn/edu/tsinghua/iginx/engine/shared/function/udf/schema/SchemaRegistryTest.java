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
import org.junit.Before;
import org.junit.Test;

public class SchemaRegistryTest {

  private SchemaRegistry registry;

  @Before
  public void setUp() {
    registry = new SchemaRegistry();
  }

  @Test
  public void testPutIfAbsentFirstTime() {
    OutputSchema schema =
        new OutputSchema(
            Collections.singletonList("col1"), Collections.singletonList(DataType.LONG), false);
    OutputSchema existing = registry.putIfAbsent("site_0", schema);
    assertNull("First insert should return null", existing);
    assertEquals(1, registry.size());
  }

  @Test
  public void testPutIfAbsentSecondTimeReturnExisting() {
    OutputSchema first =
        new OutputSchema(
            Collections.singletonList("col1"), Collections.singletonList(DataType.LONG), false);
    OutputSchema second =
        new OutputSchema(
            Arrays.asList("col1", "col2"), Arrays.asList(DataType.LONG, DataType.DOUBLE), false);

    registry.putIfAbsent("site_0", first);
    OutputSchema existing = registry.putIfAbsent("site_0", second);

    assertNotNull("Second insert should return existing", existing);
    assertTrue(existing.strictEquals(first));
    assertEquals(1, registry.size());
  }

  @Test
  public void testGetSnapshot() {
    OutputSchema schema =
        new OutputSchema(
            Collections.singletonList("x"), Collections.singletonList(DataType.DOUBLE), true);
    registry.putIfAbsent("fn_0", schema);

    OutputSchema retrieved = registry.getSnapshot("fn_0");
    assertNotNull(retrieved);
    assertTrue(retrieved.strictEquals(schema));

    assertNull(registry.getSnapshot("nonexistent"));
  }

  @Test
  public void testDifferentCallSites() {
    OutputSchema s1 =
        new OutputSchema(
            Collections.singletonList("a"), Collections.singletonList(DataType.LONG), false);
    OutputSchema s2 =
        new OutputSchema(
            Collections.singletonList("b"), Collections.singletonList(DataType.DOUBLE), false);

    registry.putIfAbsent("site_0", s1);
    registry.putIfAbsent("site_1", s2);
    assertEquals(2, registry.size());

    assertTrue(registry.getSnapshot("site_0").strictEquals(s1));
    assertTrue(registry.getSnapshot("site_1").strictEquals(s2));
  }

  @Test
  public void testClear() {
    OutputSchema schema =
        new OutputSchema(
            Collections.singletonList("a"), Collections.singletonList(DataType.LONG), false);
    registry.putIfAbsent("site_0", schema);
    assertEquals(1, registry.size());

    registry.clear();
    assertEquals(0, registry.size());
    assertNull(registry.getSnapshot("site_0"));
  }
}
