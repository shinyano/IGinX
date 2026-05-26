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
package cn.edu.tsinghua.iginx.engine.shared.function.udf.python;

import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionParams;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionType;
import cn.edu.tsinghua.iginx.engine.shared.function.MappingType;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.UDTF;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.schema.SchemaGuard;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.schema.SchemaViolationException;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.utils.ArrowDataUtils;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.utils.ArrowResultUtils;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.utils.CheckUtils;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.utils.DataUtils;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.utils.RowUtils;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import org.apache.arrow.vector.VectorSchemaRoot;
import pemja.core.PythonInterpreter;

public class PyUDTF extends PyUDF implements UDTF {

  private static final String PY_UDTF = "py_udtf";

  private final String funcName;

  public PyUDTF(String funcName, String moduleName, String className) {
    super(moduleName, className);
    this.funcName = funcName;
  }

  public PyUDTF(
      BlockingQueue<PythonInterpreter> interpreters,
      String funcName,
      String moduleName,
      String className) {
    super(interpreters, moduleName, className);
    this.funcName = funcName;
  }

  @Override
  public FunctionType getFunctionType() {
    return FunctionType.UDF;
  }

  @Override
  public MappingType getMappingType() {
    return MappingType.RowMapping;
  }

  @Override
  public String getIdentifier() {
    return PY_UDTF;
  }

  @Override
  public Row transform(Row row, FunctionParams params) throws Exception {
    if (!CheckUtils.isLegal(params)) {
      throw new IllegalArgumentException("unexpected params for PyUDTF.");
    }

    List<Object> args = params.getArgs();
    Map<String, Object> kvargs = params.getKwargs();

    if (useArrowExecution()) {
      try (VectorSchemaRoot arrowData = ArrowDataUtils.dataFromRow(row, params.getPaths())) {
        if (arrowData == null) {
          return Row.EMPTY_ROW;
        }
        try (VectorSchemaRoot result = invokePyUDFArrow(arrowData, args, kvargs)) {
          if (result == null || result.getRowCount() == 0) {
            return Row.EMPTY_ROW;
          }
          if (params.getRequestContext() != null && params.getCallSiteId() != null) {
            try {
              VectorSchemaRoot normalized =
                  SchemaGuard.ensureArrowSchema(
                      params.getRequestContext().getSchemaRegistry(),
                      params.getCallSiteId(),
                      result,
                      SchemaGuard.Policy.ALIGN_COMPATIBLE);
              if (normalized != result) {
                try (VectorSchemaRoot ignored = normalized) {
                  return ArrowResultUtils.constructSingleRow(
                      normalized, funcName, row.getKey(), row.getHeader().hasKey());
                }
              }
            } catch (SchemaViolationException e) {
              throw new Exception("UDF schema violation in " + funcName + ": " + e.getMessage(), e);
            }
          }
          return ArrowResultUtils.constructSingleRow(
              result, funcName, row.getKey(), row.getHeader().hasKey());
        }
      }
    }

    List<List<Object>> data = DataUtils.dataFromRow(row, params.getPaths());
    if (data == null) {
      return Row.EMPTY_ROW;
    }

    List<List<Object>> res = invokePyUDF(data, args, kvargs);

    if (res == null || res.size() < 3) {
      return Row.EMPTY_ROW;
    }

    // Schema Guard
    if (params.getRequestContext() != null && params.getCallSiteId() != null) {
      try {
        res =
            SchemaGuard.ensureSchema(
                params.getRequestContext().getSchemaRegistry(),
                params.getCallSiteId(),
                res,
                SchemaGuard.Policy.ALIGN_COMPATIBLE);
      } catch (SchemaViolationException e) {
        throw new Exception("UDF schema violation in " + funcName + ": " + e.getMessage(), e);
      }
    }

    boolean hasKey = res.get(0).get(0).equals("key");
    long key = -1;
    if (hasKey) {
      res.get(0).remove(0);
      res.get(1).remove(0);
      key = (Long) res.get(2).remove(0);
    }

    Header header =
        RowUtils.constructHeaderWithFirstTwoRowsUsingFuncName(
            res, row.getHeader().hasKey(), funcName);
    return RowUtils.constructNewRowWithKey(header, hasKey ? key : row.getKey(), res.get(2));
  }

  @Override
  public String getFunctionName() {
    return funcName;
  }
}
