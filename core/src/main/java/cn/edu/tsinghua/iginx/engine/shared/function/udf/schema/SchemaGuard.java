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

import static cn.edu.tsinghua.iginx.engine.shared.Constants.KEY;

import cn.edu.tsinghua.iginx.engine.shared.function.manager.ThreadInterpreterManager;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.DataTypeUtils;
import cn.edu.tsinghua.iginx.utils.TypeConverter;
import java.util.*;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * UDF 输出模式守卫。
 *
 * <p>实现 EnsureSchema 算法：首次成功返回锁定快照，后续返回相对于快照进行判断。 支持 Strict（严格相等）和 Align-Compatible（兼容规整）两种策略。
 */
public class SchemaGuard {

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaGuard.class);

  public enum Policy {
    STRICT,
    ALIGN_COMPATIBLE
  }

  private SchemaGuard() {}

  /**
   * EnsureSchema 算法主入口。
   *
   * @param registry 请求上下文中的模式注册表
   * @param callSiteId 调用点标识
   * @param rawResult UDF 返回的原始二维列表 [colNames, colTypes, row1, row2, ...]
   * @param policy 校验策略
   * @return 规范化后的二维列表，可直接进入物化阶段
   * @throws SchemaViolationException 模式不一致时抛出
   */
  public static List<List<Object>> ensureSchema(
      SchemaRegistry registry, String callSiteId, List<List<Object>> rawResult, Policy policy)
      throws SchemaViolationException {

    if (rawResult == null || rawResult.size() < 3) {
      return rawResult;
    }

    OutputSchema current = parseSchema(rawResult);

    OutputSchema existing = registry.putIfAbsent(callSiteId, current);
    if (existing == null) {
      LOGGER.debug("Schema snapshot locked for call site [{}]: {}", callSiteId, current);
      return rawResult;
    }

    OutputSchema snapshot = existing;

    if (policy == Policy.STRICT) {
      if (current.strictEquals(snapshot)) {
        return rawResult;
      }
      throw new SchemaViolationException(
          callSiteId, snapshot, current, "strict mode requires exact schema match");
    }

    // --- Align-Compatible ---

    // Rule 3.1: key semantics must match
    if (current.isHasKey() != snapshot.isHasKey()) {
      throw new SchemaViolationException(callSiteId, snapshot, current, "key semantics mismatch");
    }

    // Rule 3.3: current output must be non-empty
    if (current.getColumnNames().isEmpty()) {
      throw new SchemaViolationException(callSiteId, snapshot, current, "empty output not allowed");
    }

    // Rule 3.5: no new columns allowed
    Set<String> snapCols = snapshot.getColumnNameSet();
    Set<String> currCols = current.getColumnNameSet();
    Set<String> extraCols = new LinkedHashSet<>(currCols);
    extraCols.removeAll(snapCols);
    if (!extraCols.isEmpty()) {
      throw new SchemaViolationException(
          callSiteId, snapshot, current, "new columns not allowed: " + extraCols);
    }

    // Rule 3.4: type upcast check for common columns
    for (String col : currCols) {
      DataType srcType = current.getTypeByName(col);
      DataType targetType = snapshot.getTypeByName(col);
      if (srcType != null
          && targetType != null
          && !TypeUpcastMatrix.isUpcastAllowed(srcType, targetType)) {
        throw new SchemaViolationException(
            callSiteId,
            snapshot,
            current,
            String.format(
                "type upcast not allowed for column [%s]: %s -> %s", col, srcType, targetType));
      }
    }

    // Normalization: reorder → fill missing → upcast
    List<List<Object>> result = reorderToSnapshot(rawResult, current, snapshot);
    result = fillMissingColumnsWithNull(result, snapshot);
    result = upcastColumns(result, snapshot);

    return result;
  }

  public static VectorSchemaRoot ensureArrowSchema(
      SchemaRegistry registry, String callSiteId, VectorSchemaRoot rawResult, Policy policy)
      throws SchemaViolationException {
    if (rawResult == null || rawResult.getRowCount() == 0) {
      return rawResult;
    }

    ArrowOutputSchema current = ArrowSchemaExtractor.extract(rawResult);
    ArrowOutputSchema existing = registry.getArrowSchemaRegistry().putIfAbsent(callSiteId, current);
    if (existing == null) {
      LOGGER.debug("Schema snapshot locked for call site [{}]: {}", callSiteId, current);
      return rawResult;
    }

    ArrowOutputSchema snapshot = existing;

    if (policy == Policy.STRICT) {
      if (current.strictEquals(snapshot)) {
        return rawResult;
      }
      throw new SchemaViolationException(
          callSiteId, snapshot, current, "strict mode requires exact schema match");
    }

    if (current.isHasKey() != snapshot.isHasKey()) {
      throw new SchemaViolationException(callSiteId, snapshot, current, "key semantics mismatch");
    }

    if (current.getColumnNames().isEmpty()) {
      throw new SchemaViolationException(callSiteId, snapshot, current, "empty output not allowed");
    }

    Set<String> snapCols = snapshot.getColumnNameSet();
    Set<String> currCols = current.getColumnNameSet();
    Set<String> extraCols = new LinkedHashSet<>(currCols);
    extraCols.removeAll(snapCols);
    if (!extraCols.isEmpty()) {
      throw new SchemaViolationException(
          callSiteId, snapshot, current, "new columns not allowed: " + extraCols);
    }

    for (String col : currCols) {
      ArrowType srcType = current.getType(col);
      ArrowType targetType = snapshot.getType(col);
      if (srcType != null
          && targetType != null
          && !ArrowTypeUpcastMatrix.isAssignable(srcType, targetType)) {
        throw new SchemaViolationException(
            callSiteId,
            snapshot,
            current,
            String.format(
                "type upcast not allowed for column [%s]: %s -> %s", col, srcType, targetType));
      }
    }

    if (current.strictEquals(snapshot)) {
      return rawResult;
    }
    return reorderToSnapshot(rawResult, current, snapshot);
  }

  /** 从 UDF 原始返回中解析输出模式。 */
  static OutputSchema parseSchema(List<List<Object>> rawResult) {
    List<Object> rawNames = rawResult.get(0);
    List<Object> rawTypes = rawResult.get(1);

    boolean hasKey = !rawNames.isEmpty() && "key".equals(rawNames.get(0));
    int start = hasKey ? 1 : 0;

    List<String> colNames = new ArrayList<>();
    List<DataType> colTypes = new ArrayList<>();
    for (int i = start; i < rawNames.size(); i++) {
      colNames.add(String.valueOf(rawNames.get(i)));
      colTypes.add(DataTypeUtils.getDataTypeFromString(String.valueOf(rawTypes.get(i))));
    }

    return new OutputSchema(colNames, colTypes, hasKey);
  }

  /** 算法 2: ReorderToSnapshot — 按快照列序重排当前结果。 */
  static List<List<Object>> reorderToSnapshot(
      List<List<Object>> rawResult, OutputSchema current, OutputSchema snapshot) {

    List<String> snapNames = snapshot.getColumnNames();
    List<String> currNames = current.getColumnNames();
    boolean hasKey = current.isHasKey();

    // Build index map: column name -> position in current result (skip key at 0 if present)
    Map<String, Integer> currIndexMap = new HashMap<>();
    for (int i = 0; i < currNames.size(); i++) {
      currIndexMap.put(currNames.get(i), i + (hasKey ? 1 : 0));
    }

    List<List<Object>> result = new ArrayList<>();

    // Build new header row
    List<Object> newColNames = new ArrayList<>();
    List<Object> newColTypes = new ArrayList<>();
    if (hasKey) {
      newColNames.add("key");
      newColTypes.add(rawResult.get(1).get(0));
    }
    for (int j = 0; j < snapNames.size(); j++) {
      newColNames.add(snapNames.get(j));
      newColTypes.add(snapshot.getColumnTypes().get(j).toString());
    }
    result.add(newColNames);
    result.add(newColTypes);

    // Reorder data rows
    for (int r = 2; r < rawResult.size(); r++) {
      List<Object> srcRow = rawResult.get(r);
      List<Object> newRow = new ArrayList<>();
      if (hasKey) {
        newRow.add(srcRow.get(0));
      }
      for (String col : snapNames) {
        Integer idx = currIndexMap.get(col);
        if (idx != null && idx < srcRow.size()) {
          newRow.add(srcRow.get(idx));
        } else {
          newRow.add(null);
        }
      }
      result.add(newRow);
    }

    return result;
  }

  static VectorSchemaRoot reorderToSnapshot(
      VectorSchemaRoot rawResult, ArrowOutputSchema current, ArrowOutputSchema snapshot) {
    List<FieldVector> vectors = new ArrayList<>();
    int rowCount = rawResult.getRowCount();
    try {
      if (snapshot.isHasKey()) {
        vectors.add(copyVector(rawResult.getVector(KEY), rowCount));
      }

      for (String columnName : snapshot.getColumnNames()) {
        Field snapshotField = snapshot.getField(columnName);
        ArrowType targetType = snapshotField.getType();
        FieldVector vector =
            ArrowVectorBuilder.createVector(snapshotField, ThreadInterpreterManager.getAllocator());
        ArrowVectorBuilder.allocateVector(vector, rowCount);
        FieldVector rawVector = rawResult.getVector(columnName);
        ArrowType sourceType = current.getType(columnName);
        for (int i = 0; i < rowCount; i++) {
          Object value =
              rawVector == null ? null : ArrowVectorBuilder.readValue(rawVector, i, sourceType);
          Object converted =
              value == null ? null : ArrowValueConverter.convert(value, sourceType, targetType);
          ArrowVectorBuilder.writeValue(vector, i, targetType, converted);
        }
        vector.setValueCount(rowCount);
        vectors.add(vector);
      }

      VectorSchemaRoot result = new VectorSchemaRoot(vectors);
      result.setRowCount(rowCount);
      return result;
    } catch (RuntimeException e) {
      closeVectors(vectors);
      throw e;
    } catch (Exception e) {
      closeVectors(vectors);
      throw new RuntimeException("Failed to normalize Arrow schema result", e);
    }
  }

  /** 算法 3: FillMissingColumnsWithNull — 缺失列补空值。 */
  static List<List<Object>> fillMissingColumnsWithNull(
      List<List<Object>> result, OutputSchema snapshot) {
    // After reorder, missing columns already get null in reorderToSnapshot.
    // This method ensures the structure is consistent.
    int expectedCols = snapshot.getColumnNames().size() + (snapshot.isHasKey() ? 1 : 0);

    for (int r = 2; r < result.size(); r++) {
      List<Object> row = result.get(r);
      while (row.size() < expectedCols) {
        row.add(null);
      }
    }
    return result;
  }

  /** 算法 4: UpcastColumnsToSnapshotTypes — 类型提升。 */
  static List<List<Object>> upcastColumns(List<List<Object>> result, OutputSchema snapshot) {
    List<DataType> snapTypes = snapshot.getColumnTypes();
    boolean hasKey = snapshot.isHasKey();
    int offset = hasKey ? 1 : 0;

    for (int r = 2; r < result.size(); r++) {
      List<Object> row = result.get(r);
      for (int j = 0; j < snapTypes.size(); j++) {
        int colIdx = j + offset;
        if (colIdx < row.size() && row.get(colIdx) != null) {
          row.set(colIdx, castValue(row.get(colIdx), snapTypes.get(j)));
        }
      }
    }
    return result;
  }

  private static Object castValue(Object value, DataType targetType) {
    return TypeConverter.convertToType(targetType, value);
  }

  private static FieldVector copyVector(FieldVector rawVector, int rowCount) {
    FieldVector vector =
        ArrowVectorBuilder.createVector(
            rawVector.getField(), ThreadInterpreterManager.getAllocator());
    ArrowVectorBuilder.allocateVector(vector, rowCount);
    ArrowType type = rawVector.getField().getType();
    for (int i = 0; i < rowCount; i++) {
      Object value = ArrowVectorBuilder.readValue(rawVector, i, type);
      ArrowVectorBuilder.writeValue(vector, i, type, value);
    }
    vector.setValueCount(rowCount);
    return vector;
  }

  private static void closeVectors(List<FieldVector> vectors) {
    for (FieldVector vector : vectors) {
      try {
        vector.close();
      } catch (Exception e) {
        LOGGER.warn("Failed to close Arrow vector during schema normalization cleanup", e);
      }
    }
  }
}
