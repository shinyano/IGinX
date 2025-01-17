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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.pipeline;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
//import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.BinaryFunction;
//import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.expression.PhysicalExpression;
//import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputeException;
//import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.NotAllowArgumentTypeException;
//import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.ExecutorContext;
//import cn.edu.tsinghua.iginx.engine.shared.data.arrow.ConstantVectors;
//import cn.edu.tsinghua.iginx.engine.shared.data.arrow.ValueVectors;
//import cn.edu.tsinghua.iginx.engine.shared.data.read.Batch;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchSchema;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import com.google.common.util.concurrent.AtomicDouble;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.WillNotClose;
import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.c.jni.JniWrapper;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pemja.core.PythonInterpreter;

public class FilterExecutor  {

  /*

  private static final Logger LOGGER = LoggerFactory.getLogger(FilterExecutor.class);

  private final PhysicalExpression condition;

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private static final String PATH =
      String.join(File.separator, config.getDefaultUDFDir(), "python_scripts");

  private static final PythonInterpreter interpreter = ThreadInterpreterManager.getInterpreter();

  private static final AtomicDouble pythonInvokeTimeCounter = new AtomicDouble();

  private static final AtomicLong pythonInvokeRowCounter = new AtomicLong();

  private static final ConcurrentLinkedQueue<Pair<Integer, Double>> timeQueue =
      new ConcurrentLinkedQueue<>();

  private enum InvokeType {
    ORIGIN,
    NEW
  }

  private static final InvokeType INVOKE_TYPE = InvokeType.NEW;

  public FilterExecutor(
      ExecutorContext context, BatchSchema inputSchema, PhysicalExpression condition) {
    super(context, inputSchema);
    this.condition = Objects.requireNonNull(condition);
  }

  // TODO: use SelectionVector to filter, rather than bit vector
  @Override
  protected Batch internalCompute(Batch batch) throws ComputeException {
    List<FieldVector> results = new ArrayList<>();
    VectorFilter filter = new VectorFilter();
    switch (INVOKE_TYPE) {
      case ORIGIN:
        invokePythonOrigin(interpreter, batch.raw(), context.getAllocator());
        break;
      case NEW:
        invokePythonNew(interpreter, batch.raw(), context.getAllocator());
        break;
    }

    try (FieldVector mask = condition.invoke(context.getAllocator(), batch.raw())) {
      for (FieldVector fieldVector : batch.raw().getFieldVectors()) {
        results.add(filter.evaluate(context.getAllocator(), mask, fieldVector));
      }
    } catch (ComputeException e) {
      results.forEach(FieldVector::close);
      throw e;
    }
    return new Batch(new VectorSchemaRoot(results));
  }

  public static void invokePythonOrigin(
      PythonInterpreter interpreter, VectorSchemaRoot root, BufferAllocator allocator) {
    int rowCount = root.getRowCount();
    if (rowCount == 0) {
      return;
    }
    List<List<Object>> data = Root2DataList(root);
    List<Object> args = new ArrayList<>();
    Map<String, Object> kvargs = new HashMap<>();

    interpreter.exec("import udf_min; t=udf_min.UDFMin()");

    long start = System.nanoTime();
    List<List<Object>> res =
        (List<List<Object>>) interpreter.invokeMethod("t", "transform", data, args, kvargs);
    long end = System.nanoTime();

    System.out.println(res.toString());

    LOGGER.info(
        "original way of invoking python took {} ms for {}", (end - start) / 1000000.0, rowCount);
    pythonInvokeTimeCounter.addAndGet((end - start) / 1000000.0);
    pythonInvokeRowCounter.addAndGet(rowCount);
    timeQueue.add(new Pair<>(rowCount, (end - start) / 1000000.0));
  }

  public static void invokePythonNew(
      PythonInterpreter interpreter, VectorSchemaRoot root, BufferAllocator allocator) {
    int rowCount = root.getRowCount();
    if (rowCount == 0) {
      return;
    }
    interpreter.exec("import udaf_df_min; t=udaf_df_min.UDFMin()");
    List<Object> args = new ArrayList<>();
    Map<String, Object> kvargs = new HashMap<>();
    VectorSchemaRoot res;
    JniWrapper.get();
    System.nanoTime();

    long all_start = System.nanoTime();
    long addr = (long) interpreter.invokeMethod("t", "create_pointer");
    long end = System.nanoTime();
    long start = all_start;
    double createPointerTime = (end - start) / 1000000.0, dataExportTime, invokeTime, receiveTime;

    try (ArrowArrayStream arrowArrayStream = ArrowArrayStream.wrap(addr)) {
      start = System.nanoTime();
      ArrowRecordBatch batch =
          new VectorUnloader(root, true, true)
              .getRecordBatch();
      ArrowReader reader =
          new InMemoryArrowReader(
              allocator, root.getSchema(), new ArrayList<>(Collections.singletonList(batch)), null);
      Data.exportArrayStream(allocator, reader, arrowArrayStream);
      end = System.nanoTime();
      LOGGER.info("exported to {}", addr);
      dataExportTime = (end - start) / 1000000.0;

      start = System.nanoTime();
      addr = (long) interpreter.invokeMethod("t", "transform", addr, args, kvargs);
      end = System.nanoTime();
      invokeTime = (end - start) / 1000000.0;

      start = System.nanoTime();
      try (final ArrowArrayStream stream = ArrowArrayStream.wrap(addr);
          final ArrowReader input = Data.importArrayStream(allocator, stream)) {
        res = input.getVectorSchemaRoot();
        //        long mid = System.nanoTime();
        input.loadNextBatch();
        end = System.nanoTime();
        receiveTime = (end - start) / 1000000.0;
        //        LOGGER.info("mid1: {}, 2: {}", mid - start, end - mid);

        // 打印概要信息
        printVectorSchemaRoot(res);

        // 打印详细信息
        printVectorSchemaRootDetailed(res);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    LOGGER.info("create_pointer took {} ms for {}", createPointerTime, rowCount);
    LOGGER.info("data export took {} ms for {}", dataExportTime, rowCount);
    LOGGER.info("invoke took {} ms for {}", invokeTime, rowCount);
    LOGGER.info("receive took {} ms for {}", receiveTime, rowCount);

    LOGGER.info(
        "new way of invoking python took {} ms in total for {}",
        (end - all_start) / 1000000.0,
        root.getRowCount());
    pythonInvokeTimeCounter.addAndGet((end - all_start) / 1000000.0);
    pythonInvokeRowCounter.addAndGet(rowCount);
    timeQueue.add(new Pair<>(rowCount, (end - all_start) / 1000000.0));
  }

  public static void invokePythonNewJvm(
      PythonInterpreter interpreter, VectorSchemaRoot root, BufferAllocator allocator) {
    interpreter.exec("import udaf_df_min_jvm; t=udaf_df_min_jvm.UDFMin()");
    List<Object> args = new ArrayList<>();
    Map<String, Object> kvargs = new HashMap<>();
    VectorSchemaRoot res;

    long end, start;
    long invokeTime, receiveTime;
    int rowCount = root.getRowCount();

    start = System.nanoTime();
    long addr = (long) interpreter.invokeMethod("t", "transform", root, args, kvargs);
    end = System.nanoTime();
    invokeTime = (end - start) * 1000;
    long all_start = start;

    start = System.nanoTime();
    try (final ArrowArrayStream stream = ArrowArrayStream.wrap(addr);
        final ArrowReader input = Data.importArrayStream(allocator, stream)) {
      res = input.getVectorSchemaRoot();
      long mid = System.nanoTime();
      input.loadNextBatch();
      end = System.nanoTime();
      receiveTime = (end - start) * 1000;
      LOGGER.info("mid1: {}, 2: {}", mid - start, end - mid);

      // 打印概要信息
      printVectorSchemaRoot(res);

      // 打印详细信息
      printVectorSchemaRootDetailed(res);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    LOGGER.info("invoke took {} ms for {}", invokeTime, rowCount);
    LOGGER.info("receive took {} ms for {}", receiveTime, rowCount);

    LOGGER.info(
        "new jvm way of invoking python took {} ms in total for {}", end - all_start, rowCount);
  }

  public static void printVectorSchemaRoot(VectorSchemaRoot root) {
    StringBuilder sb = new StringBuilder("schema:\n");
    sb.append("Schema:");
    sb.append(root.getSchema());
    sb.append("\nNumber of rows: " + root.getRowCount());
    sb.append("\nColumn names:");
    for (Field field : root.getSchema().getFields()) {
      sb.append("- " + field.getName());
    }
    LOGGER.info(sb.toString());
  }

  public static void printVectorSchemaRootDetailed(VectorSchemaRoot root) {
    StringBuilder sb = new StringBuilder("detail:\n");
    sb.append("Schema:");
    sb.append(root.getSchema());
    sb.append("\nNumber of rows: " + root.getRowCount());
    sb.append("\nColumns:");

    for (int i = 0; i < root.getFieldVectors().size(); i++) {
      FieldVector vector = root.getFieldVectors().get(i);
      sb.append("\nColumn " + i + ": " + vector.getName());
      sb.append("Type: " + vector.getField().getType());
      sb.append("Data:");
      for (int j = 0; j < vector.getValueCount(); j++) {
        sb.append(vector.getObject(j));
      }
    }
    LOGGER.info(sb.toString());
  }

  public static List<List<Object>> getListData(Batch batch) {
    VectorSchemaRoot root = batch.raw();
    return Root2DataList(root);
  }

  public static List<List<Object>> Root2DataList(VectorSchemaRoot root) {
    List<List<Object>> results = new ArrayList<>();

    // Add column names as the first row
    results.add(
        root.getSchema().getFields().stream()
            .map(Field::getName)
            .collect(ArrayList::new, ArrayList::add, ArrayList::addAll));

    // Add column types as the second row
    results.add(
        root.getSchema().getFields().stream()
            .map(field -> getTypeString(field.getType()))
            .collect(ArrayList::new, ArrayList::add, ArrayList::addAll));

    // Add data rows
    int rowCount = root.getRowCount();
    for (int i = 0; i < rowCount; i++) {
      List<Object> row = new ArrayList<>();
      for (FieldVector vector : root.getFieldVectors()) {
        row.add(getValue(vector, i));
      }
      results.add(row);
    }

    return results;
  }

  private static String getTypeString(ArrowType type) {
    DataType iginxType = toIginxType(type);
    return iginxType.toString();
  }

  public static DataType toIginxType(ArrowType arrowType) {
    Objects.requireNonNull(arrowType, "arrowType");
    switch (arrowType.getTypeID()) {
      case Bool:
        return DataType.BOOLEAN;
      case Int:
        return toIginxType((ArrowType.Int) arrowType);
      case FloatingPoint:
        return toIginxType((ArrowType.FloatingPoint) arrowType);
      case Binary:
      case Utf8:
        return DataType.BINARY;
      default:
        throw new IllegalArgumentException("Unsupported arrow type: " + arrowType);
    }
  }

  public static DataType toIginxType(ArrowType.Int arrowType) {
    Objects.requireNonNull(arrowType, "arrowType");
    switch (arrowType.getBitWidth()) {
      case 32:
        return DataType.INTEGER;
      case 64:
        return DataType.LONG;
      default:
        throw new IllegalArgumentException("Unsupported arrow type: " + arrowType);
    }
  }

  public static DataType toIginxType(ArrowType.FloatingPoint arrowType) {
    Objects.requireNonNull(arrowType, "arrowType");
    switch (arrowType.getPrecision()) {
      case SINGLE:
        return DataType.FLOAT;
      case DOUBLE:
        return DataType.DOUBLE;
      default:
        throw new IllegalArgumentException("Unsupported arrow type: " + arrowType);
    }
  }

  private static Object getValue(FieldVector vector, int index) {
    if (!vector.isNull(index)) {
      if (vector instanceof IntVector) {
        return ((IntVector) vector).get(index);
      } else if (vector instanceof BigIntVector) {
        return ((BigIntVector) vector).get(index);
      } else if (vector instanceof Float4Vector) {
        return ((Float4Vector) vector).get(index);
      } else if (vector instanceof Float8Vector) {
        return ((Float8Vector) vector).get(index);
      } else if (vector instanceof VarBinaryVector) {
        return ((VarBinaryVector) vector).get(index);
      } else if (vector instanceof VarCharVector) {
        return new String(((VarCharVector) vector).get(index));
      } else if (vector instanceof BitVector) {
        return ((BitVector) vector).get(index) != 0;
      } else {
        // For any other type, return the string representation
        return vector.getObject(index).toString();
      }
    }
    return null;
  }

  @Override
  public String getInfo() {
    return "Filter(" + condition + ")";
  }

  @Override
  public void close() {
    StringBuilder sb =
        new StringBuilder(
            String.format(
                "\nInvoking python mode:%s, took %sms in total for %srows.\n",
                INVOKE_TYPE, pythonInvokeTimeCounter, pythonInvokeRowCounter));
    sb.append("Detailed information:\n");
    double tmp = 0;
    int index = 0;
    for (Pair<Integer, Double> pair : timeQueue) {
      sb.append(++index)
          .append(" attempt: ")
          .append(pair.getK())
          .append(" rows took ")
          .append(pair.getV())
          .append("ms.\n");
    }
    if (index > 1) {
      tmp = pythonInvokeTimeCounter.get() / (index - 1);
    } else if (index == 1) {
      tmp = pythonInvokeTimeCounter.get();
    }
    sb.append(
        String.format(
            "\nInvoking python mode:%s, took %sms in total for %srows.\n",
            INVOKE_TYPE, pythonInvokeTimeCounter, pythonInvokeRowCounter));
    sb.append(String.format("\navg time(except last batch) : %f.\n", tmp));
    LOGGER.info(sb.toString());
    pythonInvokeTimeCounter.set(0);
    pythonInvokeRowCounter.set(0);
    timeQueue.clear();
  }

  public static class VectorFilter extends BinaryFunction {

    public static final String NAME = "filter";

    public VectorFilter() {
      super(NAME);
    }

    @Override
    public FieldVector evaluate(
        @WillNotClose BufferAllocator allocator,
        @WillNotClose FieldVector left,
        @WillNotClose FieldVector right)
        throws ComputeException {
      if (left instanceof NullVector || right instanceof NullVector) {
        // TODO: give type for null vector
        return ConstantVectors.ofNull(
            allocator, Math.min(left.getValueCount(), right.getValueCount()));
      }
      if (left.getMinorType() != Types.MinorType.BIT) {
        throw new NotAllowArgumentTypeException(this, 0, left.getMinorType());
      }
      return evaluate(allocator, (BitVector) left, right);
    }

    public <T extends FieldVector> FieldVector evaluate(
        @WillNotClose BufferAllocator allocator, BitVector bitVector, T fieldVector) {
      T result = ValueVectors.like(allocator, fieldVector);
      int targetIndex = 0;
      for (int sourceIndex = 0; sourceIndex < bitVector.getValueCount(); sourceIndex++) {
        if (!bitVector.isNull(sourceIndex) && bitVector.get(sourceIndex) != 0) {
          result.copyFrom(sourceIndex, targetIndex, fieldVector);
          targetIndex++;
        }
      }
      result.setValueCount(targetIndex);
      return result;
    }
  }
   */
}
