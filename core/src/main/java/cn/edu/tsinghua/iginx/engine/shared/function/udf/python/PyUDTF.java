package cn.edu.tsinghua.iginx.engine.shared.function.udf.python;

import static cn.edu.tsinghua.iginx.engine.shared.Constants.UDF_CLASS;
import static cn.edu.tsinghua.iginx.engine.shared.Constants.UDF_FUNC;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionParams;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionType;
import cn.edu.tsinghua.iginx.engine.shared.function.MappingType;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.UDTF;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.utils.CheckUtils;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.utils.DataUtils;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.utils.RowUtils;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.regex.Pattern;
import pemja.core.PythonInterpreter;

public class PyUDTF implements UDTF {

  private static final String PY_UDTF = "py_udtf";

  private final BlockingQueue<PythonInterpreter> interpreters;

  private final String funcName;

  public PyUDTF(BlockingQueue<PythonInterpreter> interpreters, String funcName) {
    this.interpreters = interpreters;
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

    PythonInterpreter interpreter = interpreters.take();

//    List<Object> colNames = new ArrayList<>(Collections.singletonList("key"));
//    List<Object> colTypes = new ArrayList<>(Collections.singletonList(DataType.LONG.toString()));
//    List<Object> rowData = new ArrayList<>(Collections.singletonList(row.getKey()));
//
//    List<String> paths = params.getPaths();
//    flag:
//    for (String target : paths) {
//      if (StringUtils.isPattern(target)) {
//        Pattern pattern = Pattern.compile(StringUtils.reformatPath(target));
//        for (int i = 0; i < row.getHeader().getFieldSize(); i++) {
//          Field field = row.getHeader().getField(i);
//          if (pattern.matcher(field.getName()).matches()) {
//            colNames.add(field.getName());
//            colTypes.add(field.getType().toString());
//            rowData.add(row.getValues()[i]);
//          }
//        }
//      } else {
//        for (int i = 0; i < row.getHeader().getFieldSize(); i++) {
//          Field field = row.getHeader().getField(i);
//          if (target.equals(field.getName())) {
//            colNames.add(field.getName());
//            colTypes.add(field.getType().toString());
//            rowData.add(row.getValues()[i]);
//            continue flag;
//          }
//        }
//      }
//    }
//
//    if (colNames.size() == 1) {
//      return Row.EMPTY_ROW;
//    }
//
//    List<List<Object>> data = new ArrayList<>();
//    data.add(colNames);
//    data.add(colTypes);
//    data.add(rowData);
    List<List<Object>> data = DataUtils.dataFromTable(row, params.getPaths());
    if (data == null) {
      return Row.EMPTY_ROW;
    }

    List<Object> args = params.getArgs();
    Map<String, Object> kvargs = params.getKwargs();

    List<List<Object>> res =
        (List<List<Object>>) interpreter.invokeMethod(UDF_CLASS, UDF_FUNC, data, args, kvargs);

    if (res == null || res.size() < 3) {
      return Row.EMPTY_ROW;
    }
    interpreters.add(interpreter);

    // [["key", col1, col2 ....],
    // ["LONG", type1, type2 ...],
    // [key1, val11, val21 ...]]
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
