package cn.edu.tsinghua.iginx.engine.physical.memory.execute;

import static cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.pipeline.FilterExecutor.*;
import static cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.stateless.ProjectExecutor.invokePythonNew;
import static cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.stateless.ProjectExecutor.invokePythonNewJvm;
import static cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.stateless.ProjectExecutor.invokePythonOrigin;
import static org.junit.Assert.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.*;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.stateless.FilterExecutor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.stateless.ProjectExecutor;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.jupiter.api.AfterEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pemja.core.PythonInterpreter;
import pemja.core.PythonInterpreterConfig;

public class Batch2ListTest {
  private BufferAllocator allocator;
  private VectorSchemaRoot root;

  private static final Logger LOGGER = LoggerFactory.getLogger(Batch2ListTest.class);

  private static final String PYTHON_PATH =
      "E:\\IGinX_Lab\\local\\IGinX\\udf_funcs\\python_scripts";

  private static PythonInterpreter interpreter;

  private PythonInterpreterConfig config;

  private int ROW_COUNT = 1000000;

  @Before
  public void setUp() {
    String pythonCMD = "python";
    //    LOGGER.info(PYTHON_PATH);
    config =
        PythonInterpreterConfig.newBuilder()
            .setPythonExec(pythonCMD)
            .addPythonPaths(PYTHON_PATH)
            //                    .setExcType(PythonInterpreterConfig.ExecType.SUB_INTERPRETER)
            .build();
    interpreter = new PythonInterpreter(config);
    allocator = new RootAllocator(Long.MAX_VALUE);

    // Create schema
    Field intField = new Field("int_field", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Field longField =
        new Field("long_field", FieldType.nullable(new ArrowType.Int(64, true)), null);
    Field floatField =
        new Field(
            "float_field",
            FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
            null);
    Field doubleField =
        new Field(
            "double_field",
            FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
            null);
    Field stringField = new Field("string_field", FieldType.nullable(new ArrowType.Utf8()), null);
    Field boolField = new Field("bool_field", FieldType.nullable(new ArrowType.Bool()), null);
    //    Field dateField = new Field("date_field", FieldType.nullable(new
    // ArrowType.Date(DateUnit.DAY)), null);
    Schema schema =
        new Schema(
            Arrays.asList(intField, longField, floatField, doubleField, stringField, boolField));

    // Create vectors
    IntVector intVector = new IntVector("int_field", allocator);
    BigIntVector longVector = new BigIntVector("long_field", allocator);
    Float4Vector floatVector = new Float4Vector("float_field", allocator);
    Float8Vector doubleVector = new Float8Vector("double_field", allocator);
    VarCharVector stringVector = new VarCharVector("string_field", allocator);
    BitVector boolVector = new BitVector("bool_field", allocator);
    //    DateDayVector dateVector = new DateDayVector("date_field", allocator);

    // Populate vectors
    intVector.allocateNew(3);
    intVector.set(0, 1);
    intVector.set(1, 2);
    intVector.set(2, 3);
    intVector.setValueCount(3);

    longVector.allocateNew(3);
    longVector.set(0, 1000000000000L);
    longVector.set(1, 2000000000000L);
    longVector.set(2, 3000000000000L);
    longVector.setValueCount(3);

    floatVector.allocateNew(3);
    floatVector.set(0, 1.1f);
    floatVector.set(1, 2.2f);
    floatVector.set(2, 3.3f);
    floatVector.setValueCount(3);

    doubleVector.allocateNew(3);
    doubleVector.set(0, 1.11);
    doubleVector.set(1, 2.22);
    doubleVector.set(2, 3.33);
    doubleVector.setValueCount(3);

    stringVector.allocateNew(3);
    stringVector.set(0, "a".getBytes());
    stringVector.set(1, "b".getBytes());
    stringVector.set(2, "c".getBytes());
    stringVector.setValueCount(3);

    boolVector.allocateNew(3);
    boolVector.set(0, 1);
    boolVector.set(1, 0);
    boolVector.set(2, 1);
    boolVector.setValueCount(3);

    //    dateVector.allocateNew(3);
    //    dateVector.set(0, 18000);  // Some arbitrary date values
    //    dateVector.set(1, 18001);
    //    dateVector.set(2, 18002);
    //    dateVector.setValueCount(3);

    // Create VectorSchemaRoot
    root =
        new VectorSchemaRoot(
            schema,
            Arrays.asList(
                intVector, longVector, floatVector, doubleVector, stringVector, boolVector),
            3);
  }

  @AfterEach
  void tearDown() {
    root.close();
    allocator.close();
  }

  @Ignore
  @Test
  public void testVectorSchemaRootTo2DList() {
    List<List<Object>> result = ProjectExecutor.Root2DataList(root);

    assertEquals(5, result.size()); // 2 header rows + 3 data rows

    // Check header rows
    assertEquals(
        Arrays.asList(
            "int_field", "long_field", "float_field", "double_field", "string_field", "bool_field"),
        result.get(0));
    assertEquals(
        Arrays.asList("INTEGER", "LONG", "FLOAT", "DOUBLE", "BINARY", "BOOLEAN"), result.get(1));

    // Check data rows
    assertEquals(Arrays.asList(1, 1000000000000L, 1.1f, 1.11, "a", true), result.get(2));
    assertEquals(Arrays.asList(2, 2000000000000L, 2.2f, 2.22, "b", false), result.get(3));
    assertEquals(Arrays.asList(3, 3000000000000L, 3.3f, 3.33, "c", true), result.get(4));
  }

  @Test
  public void testTimeoffset() {
    interpreter.exec("import timeoffsettest");
    interpreter.exec("t=timeoffsettest.Test()");
    long time = System.currentTimeMillis();
    long endTime = (long) interpreter.invokeMethod("t", "test");
    LOGGER.info(String.valueOf(time));
    LOGGER.info(String.valueOf(endTime));
    LOGGER.info("{}", endTime - time);
  }

  @Test
  public void testInvokeNew() {
    try (VectorSchemaRoot vsr = createTestVSR()) {
      Thread threadA =
          new Thread(
              () -> {
                try (PythonInterpreter interpreter = new PythonInterpreter(config)) {
                  for (int i = 0; i < 10; i++) invokePythonNew(interpreter, vsr, allocator);
                }
              });
      threadA.start();
      try {
        threadA.join();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Test
  public void testInvokeNewJvm() {
    try (VectorSchemaRoot vsr = createTestVSR()) {
      Thread threadA =
          new Thread(
              () -> {
                try (PythonInterpreter interpreter = new PythonInterpreter(config)) {
                  for (int i = 0; i < 10; i++) invokePythonNewJvm(interpreter, vsr, allocator);
                }
              });
      threadA.start();
      try {
        threadA.join();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Test
  public void testInvokeOri() {
    try (VectorSchemaRoot vsr = createTestVSR()) {
      Thread threadA =
          new Thread(
              () -> {
                try (PythonInterpreter interpreter = new PythonInterpreter(config)) {
                  invokePythonOrigin(interpreter, vsr, allocator);
                }
              });
      threadA.start();
      try {
        threadA.join();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private VectorSchemaRoot createTestVSR() {
    BitVector bitVector = new BitVector("boolean", allocator);

    Map<String, String> metadata = new HashMap<>();
    metadata.put("key", "value");
    FieldType fieldType = new FieldType(true, ArrowType.Utf8.INSTANCE, null, metadata);
    VarCharVector varCharVector = new VarCharVector("varchar", fieldType, allocator);
    IntVector intVector_a =
        new IntVector("int1", FieldType.notNullable(new ArrowType.Int(32, true)), allocator);
    IntVector intVector_b =
        new IntVector("int2", FieldType.notNullable(new ArrowType.Int(32, true)), allocator);
    IntVector intVector_c =
        new IntVector("int3", FieldType.notNullable(new ArrowType.Int(32, true)), allocator);

    bitVector.allocateNew();
    varCharVector.allocateNew();
    int ran;
    for (int i = 0; i < ROW_COUNT; i++) {
      bitVector.setSafe(i, i % 2 == 0 ? 0 : 1);
      varCharVector.setSafe(i, ("test" + i).getBytes(StandardCharsets.UTF_8));
      ran = new Random().nextInt();
      intVector_a.setSafe(i, ran);
      intVector_b.setSafe(i, ran);
      intVector_c.setSafe(i, ran);
    }
    bitVector.setValueCount(ROW_COUNT);
    varCharVector.setValueCount(ROW_COUNT);
    intVector_a.setValueCount(ROW_COUNT);
    intVector_b.setValueCount(ROW_COUNT);
    intVector_c.setValueCount(ROW_COUNT);

    List<Field> fields =
        Arrays.asList(
            bitVector.getField(),
            varCharVector.getField(),
            intVector_a.getField(),
            intVector_b.getField(),
            intVector_c.getField());
    List<FieldVector> vectors =
        Arrays.asList(bitVector, varCharVector, intVector_a, intVector_b, intVector_c);

    return new VectorSchemaRoot(fields, vectors);
  }
}
