package cn.edu.tsinghua.iginx.engine.shared.function.udf.python;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionParams;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pemja.core.PythonInterpreter;
import pemja.core.PythonInterpreterConfig;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class dataTransferTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(dataTransferTest.class);

  private static PyUDSF udsf;

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private static final String PYTHON_PATH = String.join(File.separator, config.getDefaultUDFDir(), "python_scripts");

  private final String csvPath = String.join(
          File.separator,
          System.getProperty("user.dir"),
          PYTHON_PATH,
          "python_scripts",
          "web-Google.csv"
  );

  private static Table table;

  private static FunctionParams params;

  private static BlockingQueue<PythonInterpreter> queue;

  @Before
  public void init() {
    String moduleName = "udsf_pagerankall";
    String className = "UDSFpagerankall";

    String pythonCMD = "python";
    PythonInterpreterConfig config =
            PythonInterpreterConfig.newBuilder().setPythonExec(pythonCMD).addPythonPaths(PYTHON_PATH).build();
    try {
      queue = new LinkedBlockingQueue<>();
      for (int i = 0; i < 1; i++) {
        PythonInterpreter interpreter = new PythonInterpreter(config);
        interpreter.exec(String.format("import %s", moduleName));
        interpreter.exec(String.format("t = %s.%s()", moduleName, className));
        queue.add(interpreter);
      }
      initTableAndParam();
    } catch (Exception e) {
      LOGGER.error("create python interpreter failed: ", e);
    }
  }

  @Test
  public void defaultTest() {
    try {
      udsf = new PyUDSF(queue, "pagerankall");
      udsf.transform(table, params);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void initTableAndParam() {
    List<Integer[]> data = readCSV(csvPath);
    assert data != null;
    int length = data.get(0).length;
    List<Field> fields = new ArrayList<>();
    List<Pair<Integer, Object>> paths = new ArrayList<>();
    for (int i = 1; i < length; i++) {
      fields.add(new Field("col_" + i, DataType.INTEGER));
      paths.add(new Pair<>(0, "col_" + i));
    }
    Header header = new Header(Field.KEY, fields);
    List<Row> rows = new ArrayList<>();
    for (Integer[] row : data) {
      rows.add(new Row(header, row));
    }
    table = new Table(header, rows);
    params = new FunctionParams(paths,  new HashMap<>(), false);
  }

  public static List<Integer[]> readCSV(String path) {
    try(FileReader fileReader = new FileReader(path)) {
      CSVParser csvParser = new CSVParser(fileReader, CSVFormat.DEFAULT);
      List<Integer[]> res = new ArrayList<>();
      for (CSVRecord record : csvParser) {
        // 逐行处理CSV数据，这里可以根据需要进行处理
        Integer[] row = new Integer[record.size()];
        int i = 0;
        for (String value : record) {
          row[i++] = Integer.parseInt(value);
        }
        res.add(row);
      }
      csvParser.close();
      return res;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }


}
