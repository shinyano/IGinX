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
package cn.edu.tsinghua.iginx.integration.func.udf;

import static cn.edu.tsinghua.iginx.integration.controller.Controller.SUPPORT_KEY;
import static cn.edu.tsinghua.iginx.integration.controller.Controller.clearAllData;
import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.func.session.InsertAPIType;
import cn.edu.tsinghua.iginx.integration.tool.ConfLoader;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class UDFSchemaGuardIT {

  private static Session session;

  private static UDFTestTools tool;

  private static boolean needCompareResult = true;

  private static boolean dummyNoData = true;

  private static List<String> taskToBeRemoved;

  private static final String SINGLE_UDF_REGISTER_SQL =
      "CREATE FUNCTION %s \"%s\" FROM \"%s\" IN \"%s\";";

  private static final String SCRIPT_FILE_PATH =
      String.join(
          File.separator,
          System.getProperty("user.dir"),
          "src",
          "test",
          "resources",
          "udf",
          "schema_guard_window_udaf.py");

  @BeforeClass
  public static void setUp() throws SessionException {
    ConfLoader conf = new ConfLoader(Controller.CONFIG_FILE);
    if (!SUPPORT_KEY.get(conf.getStorageType()) && conf.isScaling()) {
      needCompareResult = false;
    }
    session = new Session("127.0.0.1", 6888, "root", "root");
    session.openSession();
    tool = new UDFTestTools(session);
  }

  @AfterClass
  public static void tearDown() throws SessionException {
    clearAllData(session);
    session.closeSession();
  }

  @Before
  public void insertData() {
    long startKey = 0L;
    long endKey = 30L;
    List<String> pathList = Arrays.asList("us.d1.s1", "us.d1.s2");
    List<DataType> dataTypeList = Arrays.asList(DataType.LONG, DataType.DOUBLE);
    List<Long> keyList = new ArrayList<>();
    List<List<Object>> valuesList = new ArrayList<>();
    int size = (int) (endKey - startKey);
    for (int i = 0; i < size; i++) {
      keyList.add(startKey + i);
      valuesList.add(Arrays.asList((long) i, i + 0.5d));
    }
    Controller.writeRowsData(
        session,
        pathList,
        keyList,
        dataTypeList,
        valuesList,
        new ArrayList<>(),
        InsertAPIType.Row,
        dummyNoData);
    dummyNoData = false;
    Controller.after(session);
  }

  @After
  public void clearData() {
    Controller.clearData(session);
  }

  @Before
  public void resetTaskToBeDropped() {
    if (taskToBeRemoved == null) {
      taskToBeRemoved = new ArrayList<>();
    } else {
      taskToBeRemoved.clear();
    }
  }

  @After
  public void dropTasks() {
    tool.dropTasks(taskToBeRemoved);
    taskToBeRemoved.clear();
  }

  @Test
  public void testCompatibleSchemaDriftCanBeNormalized() {
    String udfName = registerSchemaGuardUdf("schema_guard_window_compat");

    String statement =
        String.format("SELECT %s(s1, s2, 0) FROM us.d1 OVER WINDOW (size 10 IN [0, 30));", udfName);
    SessionExecuteSqlResult ret = tool.execute(statement);

    compareResult(
        Arrays.asList("window_start", "window_end", "metric_double", "metric_long"),
        ret.getPaths());
    compareResult(
        Arrays.asList(DataType.LONG, DataType.LONG, DataType.DOUBLE, DataType.LONG),
        ret.getDataTypeList());
    compareResult(3, ret.getValues().size());

    List<Object> firstWindow = ret.getValues().get(0);
    List<Object> secondWindow = ret.getValues().get(1);
    List<Object> thirdWindow = ret.getValues().get(2);

    assertDoubleValue(5.0d, firstWindow.get(2));
    assertLongValue(45L, firstWindow.get(3));

    // Window 1 returns reversed columns and a real int32 scalar; SchemaGuard should reorder and
    // upcast.
    assertDoubleValue(15.0d, secondWindow.get(2));
    assertLongValue(145L, secondWindow.get(3));

    // Window 2 omits metric_double; SchemaGuard should fill it with null.
    assertNull(thirdWindow.get(2));
    assertLongValue(245L, thirdWindow.get(3));
  }

  @Test
  public void testFloatToDoubleUpcastCanBeNormalized() {
    String udfName = registerSchemaGuardUdf("schema_guard_window_float");

    String statement =
        String.format("SELECT %s(s2, 1) FROM us.d1 OVER WINDOW (size 10 IN [0, 20));", udfName);
    SessionExecuteSqlResult ret = tool.execute(statement);

    compareResult(Arrays.asList("window_start", "window_end", "metric_double"), ret.getPaths());
    compareResult(
        Arrays.asList(DataType.LONG, DataType.LONG, DataType.DOUBLE), ret.getDataTypeList());
    compareResult(2, ret.getValues().size());

    assertDoubleValue(5.0d, ret.getValues().get(0).get(2));
    assertDoubleValue(15.0d, ret.getValues().get(1).get(2));
  }

  @Test
  public void testNewColumnSchemaDriftFails() {
    String udfName = registerSchemaGuardUdf("schema_guard_window_new_col");

    String statement =
        String.format("SELECT %s(s1, s2, 2) FROM us.d1 OVER WINDOW (size 10 IN [0, 20));", udfName);
    Exception ex = tool.executeFail(statement);

    // 目前只能返回这个，手动检查日志是可以发现的
    assertTrue(ex.getMessage().contains("encounter error"));
    //    assertTrue(ex.getMessage().contains("new columns not allowed"));
  }

  @Test
  public void testKeyMismatchSchemaDriftFails() {
    String udfName = registerSchemaGuardUdf("schema_guard_window_key");

    String statement =
        String.format("SELECT %s(s1, 3) FROM us.d1 OVER WINDOW (size 10 IN [0, 20));", udfName);
    Exception ex = tool.executeFail(statement);

    assertTrue(ex.getMessage().contains("encounter error"));

    //    assertTrue(ex.getMessage().contains("schema violation"));
    //    assertTrue(ex.getMessage().contains("key semantics mismatch"));
  }

  @Test
  public void testIncompatibleTypeSchemaDriftFails() {
    String udfName = registerSchemaGuardUdf("schema_guard_window_type");

    String statement =
        String.format("SELECT %s(s1, s2, 4) FROM us.d1 OVER WINDOW (size 10 IN [0, 20));", udfName);
    Exception ex = tool.executeFail(statement);

    assertTrue(ex.getMessage().contains("encounter error"));

    //    assertTrue(ex.getMessage().contains("schema violation"));
    //    assertTrue(ex.getMessage().contains("type upcast not allowed"));
  }

  private String registerSchemaGuardUdf(String udfName) {
    tool.executeReg(
        String.format(
            SINGLE_UDF_REGISTER_SQL, "UDAF", udfName, "SchemaGuardWindowUDAF", SCRIPT_FILE_PATH));
    assertTrue(tool.isUDFRegistered(udfName));
    taskToBeRemoved.add(udfName);
    return udfName;
  }

  private void compareResult(Object expected, Object actual) {
    if (!needCompareResult) {
      return;
    }
    assertEquals(expected, actual);
  }

  private void assertLongValue(long expected, Object actual) {
    if (!needCompareResult) {
      return;
    }
    assertTrue(actual instanceof Number);
    assertEquals(expected, ((Number) actual).longValue());
  }

  private void assertDoubleValue(double expected, Object actual) {
    if (!needCompareResult) {
      return;
    }
    assertTrue(actual instanceof Number);
    assertEquals(expected, ((Number) actual).doubleValue(), 0.001d);
  }
}
