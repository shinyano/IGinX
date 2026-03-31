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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

public class SchemaGuardTest {

  private SchemaRegistry registry;

  @Before
  public void setUp() {
    registry = new SchemaRegistry();
  }

  // ================== helper ==================

  /**
   * Build a raw result: [[colName1, colName2, ...], [type1, type2, ...], [val1, val2, ...], ...]
   */
  private static List<List<Object>> buildResult(
      List<String> colNames, List<String> colTypes, List<List<Object>> rows) {
    List<List<Object>> result = new ArrayList<>();
    result.add(new ArrayList<>(colNames));
    result.add(new ArrayList<>(colTypes));
    for (List<Object> row : rows) {
      result.add(new ArrayList<>(row));
    }
    return result;
  }

  // ================== STRICT policy tests ==================

  @Test
  public void testStrictFirstCallLocksSnapshot() throws SchemaViolationException {
    List<List<Object>> result =
        buildResult(
            Arrays.asList("col_a", "col_b"),
            Arrays.asList("LONG", "DOUBLE"),
            Arrays.asList(Arrays.asList(1L, 2.0)));

    List<List<Object>> out =
        SchemaGuard.ensureSchema(registry, "fn_0", result, SchemaGuard.Policy.STRICT);

    assertNotNull(out);
    assertNotNull(registry.getSnapshot("fn_0"));
    assertSame("First call should return original result", result, out);
  }

  @Test
  public void testStrictIdenticalSchemaPass() throws SchemaViolationException {
    List<List<Object>> first =
        buildResult(
            Arrays.asList("c1", "c2"),
            Arrays.asList("LONG", "DOUBLE"),
            Arrays.asList(Arrays.asList(1L, 1.0)));

    List<List<Object>> second =
        buildResult(
            Arrays.asList("c1", "c2"),
            Arrays.asList("LONG", "DOUBLE"),
            Arrays.asList(Arrays.asList(2L, 2.0)));

    SchemaGuard.ensureSchema(registry, "fn_0", first, SchemaGuard.Policy.STRICT);
    List<List<Object>> out =
        SchemaGuard.ensureSchema(registry, "fn_0", second, SchemaGuard.Policy.STRICT);

    assertNotNull(out);
  }

  @Test(expected = SchemaViolationException.class)
  public void testStrictDifferentColNameFails() throws SchemaViolationException {
    List<List<Object>> first =
        buildResult(
            Arrays.asList("c1", "c2"),
            Arrays.asList("LONG", "DOUBLE"),
            Arrays.asList(Arrays.asList(1L, 1.0)));

    List<List<Object>> second =
        buildResult(
            Arrays.asList("c1", "c3"),
            Arrays.asList("LONG", "DOUBLE"),
            Arrays.asList(Arrays.asList(2L, 2.0)));

    SchemaGuard.ensureSchema(registry, "fn_0", first, SchemaGuard.Policy.STRICT);
    SchemaGuard.ensureSchema(registry, "fn_0", second, SchemaGuard.Policy.STRICT);
  }

  @Test(expected = SchemaViolationException.class)
  public void testStrictDifferentTypeFails() throws SchemaViolationException {
    List<List<Object>> first =
        buildResult(
            Arrays.asList("c1"), Arrays.asList("LONG"), Arrays.asList(Arrays.<Object>asList(1L)));

    List<List<Object>> second =
        buildResult(
            Arrays.asList("c1"), Arrays.asList("INTEGER"), Arrays.asList(Arrays.<Object>asList(1)));

    SchemaGuard.ensureSchema(registry, "fn_0", first, SchemaGuard.Policy.STRICT);
    SchemaGuard.ensureSchema(registry, "fn_0", second, SchemaGuard.Policy.STRICT);
  }

  // ================== ALIGN_COMPATIBLE policy tests ==================

  @Test
  public void testAlignCompatibleFirstCallLocksSnapshot() throws SchemaViolationException {
    List<List<Object>> result =
        buildResult(
            Arrays.asList("x", "y"),
            Arrays.asList("LONG", "DOUBLE"),
            Arrays.asList(Arrays.asList(1L, 2.0)));

    SchemaGuard.ensureSchema(registry, "fn_0", result, SchemaGuard.Policy.ALIGN_COMPATIBLE);
    assertNotNull(registry.getSnapshot("fn_0"));
  }

  @Test
  public void testAlignCompatibleReorderColumns() throws SchemaViolationException {
    // First call: snapshot locks column order as [a, b]
    List<List<Object>> first =
        buildResult(
            Arrays.asList("a", "b"),
            Arrays.asList("LONG", "DOUBLE"),
            Arrays.asList(Arrays.asList(1L, 2.0)));

    SchemaGuard.ensureSchema(registry, "fn_0", first, SchemaGuard.Policy.ALIGN_COMPATIBLE);

    // Second call: columns in reversed order [b, a]
    List<List<Object>> second =
        buildResult(
            Arrays.asList("b", "a"),
            Arrays.asList("DOUBLE", "LONG"),
            Arrays.asList(Arrays.asList(4.0, 3L)));

    List<List<Object>> out =
        SchemaGuard.ensureSchema(registry, "fn_0", second, SchemaGuard.Policy.ALIGN_COMPATIBLE);

    // Should be reordered to [a, b]
    assertEquals(Arrays.asList("a", "b"), out.get(0));
    assertEquals(3L, ((Number) out.get(2).get(0)).longValue());
    assertEquals(4.0, ((Number) out.get(2).get(1)).doubleValue(), 0.001);
  }

  @Test
  public void testAlignCompatibleFillMissingColumns() throws SchemaViolationException {
    // Snapshot has [a, b]
    List<List<Object>> first =
        buildResult(
            Arrays.asList("a", "b"),
            Arrays.asList("LONG", "DOUBLE"),
            Arrays.asList(Arrays.asList(1L, 2.0)));

    SchemaGuard.ensureSchema(registry, "fn_0", first, SchemaGuard.Policy.ALIGN_COMPATIBLE);

    // Second call only has [a], missing [b]
    List<List<Object>> second =
        buildResult(
            Arrays.asList("a"), Arrays.asList("LONG"), Arrays.asList(Arrays.<Object>asList(5L)));

    List<List<Object>> out =
        SchemaGuard.ensureSchema(registry, "fn_0", second, SchemaGuard.Policy.ALIGN_COMPATIBLE);

    // Should have [a, b], b filled with null
    assertEquals(Arrays.asList("a", "b"), out.get(0));
    assertEquals(5L, ((Number) out.get(2).get(0)).longValue());
    assertNull(out.get(2).get(1));
  }

  @Test
  public void testAlignCompatibleUpcastIntegerToLong() throws SchemaViolationException {
    // Snapshot type is LONG
    List<List<Object>> first =
        buildResult(
            Arrays.asList("val"),
            Arrays.asList("LONG"),
            Arrays.asList(Arrays.<Object>asList(100L)));

    SchemaGuard.ensureSchema(registry, "fn_0", first, SchemaGuard.Policy.ALIGN_COMPATIBLE);

    // Second call returns INTEGER
    List<List<Object>> second =
        buildResult(
            Arrays.asList("val"),
            Arrays.asList("INTEGER"),
            Arrays.asList(Arrays.<Object>asList(42)));

    List<List<Object>> out =
        SchemaGuard.ensureSchema(registry, "fn_0", second, SchemaGuard.Policy.ALIGN_COMPATIBLE);

    // Should be upcast to Long
    Object value = out.get(2).get(0);
    assertTrue("Value should be Long after upcast", value instanceof Long);
    assertEquals(42L, ((Long) value).longValue());
  }

  @Test
  public void testAlignCompatibleUpcastFloatToDouble() throws SchemaViolationException {
    // Snapshot type is DOUBLE
    List<List<Object>> first =
        buildResult(
            Arrays.asList("val"),
            Arrays.asList("DOUBLE"),
            Arrays.asList(Arrays.<Object>asList(1.0)));

    SchemaGuard.ensureSchema(registry, "fn_0", first, SchemaGuard.Policy.ALIGN_COMPATIBLE);

    // Second call returns FLOAT
    List<List<Object>> second =
        buildResult(
            Arrays.asList("val"),
            Arrays.asList("FLOAT"),
            Arrays.asList(Arrays.<Object>asList(3.14f)));

    List<List<Object>> out =
        SchemaGuard.ensureSchema(registry, "fn_0", second, SchemaGuard.Policy.ALIGN_COMPATIBLE);

    Object value = out.get(2).get(0);
    assertTrue("Value should be Double after upcast", value instanceof Double);
    assertEquals(3.14, ((Double) value).doubleValue(), 0.01);
  }

  @Test(expected = SchemaViolationException.class)
  public void testAlignCompatibleKeyMismatchFails() throws SchemaViolationException {
    List<List<Object>> first =
        buildResult(
            Arrays.asList("key", "val"),
            Arrays.asList("LONG", "DOUBLE"),
            Arrays.asList(Arrays.asList(0L, 1.0)));

    SchemaGuard.ensureSchema(registry, "fn_0", first, SchemaGuard.Policy.ALIGN_COMPATIBLE);

    // Second: no key
    List<List<Object>> second =
        buildResult(
            Arrays.asList("val"),
            Arrays.asList("DOUBLE"),
            Arrays.asList(Arrays.<Object>asList(2.0)));

    SchemaGuard.ensureSchema(registry, "fn_0", second, SchemaGuard.Policy.ALIGN_COMPATIBLE);
  }

  @Test(expected = SchemaViolationException.class)
  public void testAlignCompatibleNewColumnFails() throws SchemaViolationException {
    List<List<Object>> first =
        buildResult(
            Arrays.asList("a"), Arrays.asList("LONG"), Arrays.asList(Arrays.<Object>asList(1L)));

    SchemaGuard.ensureSchema(registry, "fn_0", first, SchemaGuard.Policy.ALIGN_COMPATIBLE);

    // Second: adds new column "b"
    List<List<Object>> second =
        buildResult(
            Arrays.asList("a", "b"),
            Arrays.asList("LONG", "DOUBLE"),
            Arrays.asList(Arrays.asList(2L, 3.0)));

    SchemaGuard.ensureSchema(registry, "fn_0", second, SchemaGuard.Policy.ALIGN_COMPATIBLE);
  }

  @Test(expected = SchemaViolationException.class)
  public void testAlignCompatibleIncompatibleUpcastFails() throws SchemaViolationException {
    // Snapshot: INTEGER
    List<List<Object>> first =
        buildResult(
            Arrays.asList("val"),
            Arrays.asList("INTEGER"),
            Arrays.asList(Arrays.<Object>asList(1)));

    SchemaGuard.ensureSchema(registry, "fn_0", first, SchemaGuard.Policy.ALIGN_COMPATIBLE);

    // Second: DOUBLE (cross-family, not allowed)
    List<List<Object>> second =
        buildResult(
            Arrays.asList("val"),
            Arrays.asList("DOUBLE"),
            Arrays.asList(Arrays.<Object>asList(2.0)));

    SchemaGuard.ensureSchema(registry, "fn_0", second, SchemaGuard.Policy.ALIGN_COMPATIBLE);
  }

  // ================== edge cases ==================

  @Test
  public void testNullResultPassesThrough() throws SchemaViolationException {
    assertNull(
        SchemaGuard.ensureSchema(registry, "fn_0", null, SchemaGuard.Policy.ALIGN_COMPATIBLE));
  }

  @Test
  public void testEmptyResultPassesThrough() throws SchemaViolationException {
    List<List<Object>> empty = new ArrayList<>();
    assertSame(
        empty,
        SchemaGuard.ensureSchema(registry, "fn_0", empty, SchemaGuard.Policy.ALIGN_COMPATIBLE));
  }

  @Test
  public void testTwoRowsOnlyPassesThrough() throws SchemaViolationException {
    List<List<Object>> headerOnly = new ArrayList<>();
    headerOnly.add(Arrays.asList("a"));
    headerOnly.add(Arrays.asList("LONG"));
    assertSame(
        headerOnly,
        SchemaGuard.ensureSchema(
            registry, "fn_0", headerOnly, SchemaGuard.Policy.ALIGN_COMPATIBLE));
  }

  @Test
  public void testWithKeyColumn() throws SchemaViolationException {
    List<List<Object>> first =
        buildResult(
            Arrays.asList("key", "val"),
            Arrays.asList("LONG", "DOUBLE"),
            Arrays.asList(Arrays.asList(0L, 1.0)));

    SchemaGuard.ensureSchema(registry, "fn_0", first, SchemaGuard.Policy.ALIGN_COMPATIBLE);

    List<List<Object>> second =
        buildResult(
            Arrays.asList("key", "val"),
            Arrays.asList("LONG", "DOUBLE"),
            Arrays.asList(Arrays.asList(1L, 2.0)));

    List<List<Object>> out =
        SchemaGuard.ensureSchema(registry, "fn_0", second, SchemaGuard.Policy.ALIGN_COMPATIBLE);

    assertEquals("key", out.get(0).get(0));
    assertEquals("val", out.get(0).get(1));
    assertEquals(1L, out.get(2).get(0));
  }

  @Test
  public void testDifferentCallSitesAreIndependent() throws SchemaViolationException {
    List<List<Object>> r1 =
        buildResult(
            Arrays.asList("a"), Arrays.asList("LONG"), Arrays.asList(Arrays.<Object>asList(1L)));

    List<List<Object>> r2 =
        buildResult(
            Arrays.asList("b"), Arrays.asList("DOUBLE"), Arrays.asList(Arrays.<Object>asList(2.0)));

    SchemaGuard.ensureSchema(registry, "fn_0", r1, SchemaGuard.Policy.STRICT);
    SchemaGuard.ensureSchema(registry, "fn_1", r2, SchemaGuard.Policy.STRICT);

    assertEquals(2, registry.size());
  }

  @Test
  public void testParseSchema() {
    List<List<Object>> rawResult = new ArrayList<>();
    rawResult.add(Arrays.asList("key", "col_a", "col_b"));
    rawResult.add(Arrays.asList("LONG", "INTEGER", "DOUBLE"));
    rawResult.add(Arrays.asList(0L, 1, 2.0));

    OutputSchema schema = SchemaGuard.parseSchema(rawResult);
    assertTrue(schema.isHasKey());
    assertEquals(Arrays.asList("col_a", "col_b"), schema.getColumnNames());
  }

  @Test
  public void testMultiRowReorderAndFill() throws SchemaViolationException {
    // Snapshot: [a, b, c]
    List<List<Object>> first =
        buildResult(
            Arrays.asList("a", "b", "c"),
            Arrays.asList("LONG", "DOUBLE", "LONG"),
            Arrays.asList(Arrays.asList(1L, 2.0, 3L)));

    SchemaGuard.ensureSchema(registry, "fn_0", first, SchemaGuard.Policy.ALIGN_COMPATIBLE);

    // Second: only [c, a], missing b, reversed order, multiple rows
    List<List<Object>> second =
        buildResult(
            Arrays.asList("c", "a"),
            Arrays.asList("LONG", "LONG"),
            Arrays.asList(Arrays.<Object>asList(30L, 10L), Arrays.<Object>asList(60L, 40L)));

    List<List<Object>> out =
        SchemaGuard.ensureSchema(registry, "fn_0", second, SchemaGuard.Policy.ALIGN_COMPATIBLE);

    // Reordered to [a, b, c], b = null
    assertEquals(Arrays.asList("a", "b", "c"), out.get(0));
    // Row 0
    assertEquals(10L, ((Number) out.get(2).get(0)).longValue());
    assertNull(out.get(2).get(1));
    assertEquals(30L, ((Number) out.get(2).get(2)).longValue());
    // Row 1
    assertEquals(40L, ((Number) out.get(3).get(0)).longValue());
    assertNull(out.get(3).get(1));
    assertEquals(60L, ((Number) out.get(3).get(2)).longValue());
  }
}
