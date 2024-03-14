package cn.edu.tsinghua.iginx.engine.shared.function.udf.python;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionParams;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.arrow.FlightReader;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.arrow.FlightWriter;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.transform.data.BatchData;
import cn.edu.tsinghua.iginx.utils.Pair;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pemja.core.PythonInterpreter;
import pemja.core.PythonInterpreterConfig;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class dataTransferTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(dataTransferTest.class);

  private static PyUDSF udsf;

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private static final String PYTHON_PATH = String.join(File.separator, "..", config.getDefaultUDFDir(), "python_scripts");

  private final String csvPath = String.join(
          File.separator,
          System.getProperty("user.dir"),
          PYTHON_PATH,
          "web-Google.csv"
  );

  private static Table table;

  private static List<List<Object>> data = new ArrayList<>();

  private static List<List<Object>> posParams;

  private static Map<String, Object> kvargs;

  private static VectorSchemaRoot root;

  private static FunctionParams params;

  private static BlockingQueue<PythonInterpreter> oldQueue;
  private static BlockingQueue<PythonInterpreter> newQueue;

  private static PythonInterpreter flightServerInterpreter;

  private static final int COL_COUNT = 5;

//  private static final int ROW_COUNT = 10;
  private static final int ROW_COUNT = 10000000;

  @Before
  public void init() {
    // old module: before udf structure refaction
    // new module: after refaction
    String moduleName = "mock_udf";
    String moduleNameNew = "mock_udf_new";
    String className = "MockUDF";

    String pythonCMD = "python";
    PythonInterpreterConfig config =
            PythonInterpreterConfig.newBuilder().setPythonExec(pythonCMD).addPythonPaths(PYTHON_PATH).build();
    try {
      oldQueue = new LinkedBlockingQueue<>();
      newQueue = new LinkedBlockingQueue<>();
      for (int i = 0; i < 10; i++) {
        PythonInterpreter oldInterpreter = new PythonInterpreter(config);
        oldInterpreter.exec("import iginx_udf");
        oldInterpreter.exec(String.format("import %s", moduleName));
        oldInterpreter.exec(String.format("t = %s.%s()", moduleName, className));
        oldQueue.add(oldInterpreter);
        PythonInterpreter newInterpreter = new PythonInterpreter(config);
        newInterpreter.exec("import iginx_udf");
        newInterpreter.exec(String.format("import %s", moduleNameNew));
        newInterpreter.exec(String.format("t = %s.%s()", moduleNameNew, className));
        newQueue.add(newInterpreter);
      }
      flightServerInterpreter = new PythonInterpreter(config);
      flightServerInterpreter.exec("from iginx_udf.arrow.flight_server import start_server, shutdown_server");
//      flightServerInterpreter.exec("start_server()");
      try {
        ProcessBuilder builder = new ProcessBuilder();
        builder.command("python", String.join(File.separator, PYTHON_PATH, "iginx_udf", "arrow", "flight_server.py"));

//        builder.directory(new File("/path/to/working_directory"));
        builder.redirectErrorStream(true);

        Process process = builder.start();
//        // 异步读取标准输出
//        new Thread(() -> {
//          try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
//            String line;
//            while ((line = reader.readLine()) != null) {
//              System.out.println(line);
//            }
//          } catch (IOException e) {
//            e.printStackTrace();
//          }
//        }).start();
//        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
//        String line;
//        while ((line = reader.readLine()) != null) {
//          System.out.println(line);
//        }
//
//        int exitCode = process.waitFor();
//        System.out.println("Exit code: " + exitCode);
      } catch (IOException e) {
        e.printStackTrace();
      }
//      initTableAndParam();
      initDirectDataAndParam();
    } catch (Exception e) {
      LOGGER.error("create python interpreter failed: ", e);
    }
  }

  @After
  public void stop() {
    data = null;
    data = new ArrayList<>();
    table = null;
    root = null;
//    flightServerInterpreter.exec("shutdown_server()");
  }

// TODO: full tests
//  @Test
//  public void defaultTest() {
//    try {
//      PyUDSF udsf = new PyUDSF(oldQueue, "mock_udf");
//      udsf.transform(table, params);
//    } catch (Exception e) {
//      throw new RuntimeException(e);
//    }
//  }
//
//  @Test
//  public void FlightUDFTest() {
//    try {
//      PyUDSFNew udsf = new PyUDSFNew(oldQueue, "flight_udf");
//      udsf.transform(table, params);
//    } catch (Exception e) {
//      throw new RuntimeException(e);
//    }
//  }

  @Test
  public void defaultTests() {
    for (int i = 0; i < 10; i++) {
      if (i != 0) {
        initRoot();
      }
//      defaultNewDirectTest();
//      defaultOldDirectTest();
      flightSendDirectTest();
    }
  }

  @Test
  public void defaultOldDirectTest() {
    try {
      PythonInterpreter oldInterpreter = oldQueue.take();
      long startTime = System.currentTimeMillis();
      List<List<Object>> res = (List<List<Object>>) oldInterpreter.invokeMethod("t", "transform", data, posParams, kvargs);
      long endTime = System.currentTimeMillis();
      LOGGER.info("defaultDirectTest(old module): invoke operation took {} ms.", endTime - startTime);
      oldQueue.add(oldInterpreter);
    } catch (InterruptedException e) {
      LOGGER.error("Failed to take interpreter.", e);
    }
  }

  @Test
  public void defaultNewDirectTest() {
    try {
      PythonInterpreter newInterpreter = newQueue.take();
      long startTime = System.currentTimeMillis();
      List<List<Object>> res = (List<List<Object>>) newInterpreter.invokeMethod("t", "transform", data, posParams, kvargs);
      long endTime = System.currentTimeMillis();
      LOGGER.info("defaultDirectTest(new module): invoke operation took {} ms.", endTime - startTime);
      newQueue.add(newInterpreter);
    } catch (InterruptedException e) {
      LOGGER.error("Failed to take interpreter.", e);
    }
  }

  @Test
  public void flightSendDirectTest() {
    assert root.getRowCount() == ROW_COUNT;
    assert data.size() == ROW_COUNT + 2;
    FlightWriter writer = new FlightWriter("mock_udf_path");
    try {
      PythonInterpreter newInterpreter = newQueue.take();
      LOGGER.info("flight started sending data.");
      long startTime = System.currentTimeMillis();
      writer.sendData(root);
      long endTime = System.currentTimeMillis();
      LOGGER.info("flightDirectTest: sendData operation took {} ms.", endTime - startTime);
//      FlightClient flightClient = FlightClient.builder(new RootAllocator(Long.MAX_VALUE), Location.forGrpcInsecure("localhost", 33333)).build();
//      LOGGER.info("Client (Location): Connected to {}", Location.forGrpcInsecure("localhost", 33333));
//      FlightInfo flightInfo = flightClient.getInfo(FlightDescriptor.path("mock_udf_path"));
//      System.out.println("C3: Client (Get Metadata): " + flightInfo);


//      flightServerInterpreter.exec("import pyarrow.flight as flight");
//      flightServerInterpreter.exec("client = flight.connect('grpc://localhost:33333')");

      ProcessBuilder processBuilder = new ProcessBuilder("python", String.join(File.separator, PYTHON_PATH, "mock_udf_new.py"), "mock_udf_path");
      processBuilder.redirectErrorStream(true);

      try {
        Process process = processBuilder.start();
        String output = new BufferedReader(new InputStreamReader(process.getInputStream()))
                .lines().collect(Collectors.joining("\n"));
        System.out.println(output);

        int exitCode = process.waitFor();
        System.out.println("\nExited with error code : " + exitCode);

      } catch (IOException | InterruptedException e) {
        e.printStackTrace();
      }

//      flightServerInterpreter.exec("from iginx_udf.arrow.test import get_data");
//      flightServerInterpreter.exec("get_data()");
//      List<List<Object>> res = (List<List<Object>>) newInterpreter.invokeMethod("t", "flight_transform", "mock_udf_path", posParams, kvargs);
//      ExecutorService executor = Executors.newSingleThreadExecutor();
//
//      Future<?> future = executor.submit(new Runnable() {
//        @Override
//        public void run() {
//          long startTime = System.currentTimeMillis();
//          flightServerInterpreter.exec("from iginx_udf.arrow.test import get_data");
//          flightServerInterpreter.exec("get_data()");
//          List<List<Object>> res = (List<List<Object>>) newInterpreter.invokeMethod("t", "flight_transform", "mock_udf_path", posParams, kvargs);
//          long endTime = System.currentTimeMillis();
//          LOGGER.info("flightDirectTest: invoke operation took {} ms.", endTime - startTime);
//          // 在这里执行任务
////          System.out.println("Task is running.");
////          try {
////            Thread.sleep(2000); // 模拟执行任务
////          } catch (InterruptedException e) {
////            Thread.currentThread().interrupt();
////          }
////          System.out.println("Task has finished.");
//        }
//      });
//
//      future.get();
//
//      // 关闭 ExecutorService
//      executor.shutdown();
//
//      System.out.println("Main thread is now finishing.");
      newQueue.add(newInterpreter);
    } catch (InterruptedException e) {
      LOGGER.error("Failed to take interpreter.", e);
    } finally {
      root.close();
//      writer.clearData();
//      writer.shutdown();
      LOGGER.info("Exited.");
    }
  }

  @Test
  public void FLightReceiveDirectTest() {
    FlightReader reader = new FlightReader("mock_udf_path");
    long startTime = System.currentTimeMillis();
    reader.read();
    long endTime = System.currentTimeMillis();
    LOGGER.info("FLightReceiveDirectTest: read data operation took {} ms.", endTime - startTime);
  }

  @Test
  public void OnlyFlight() {
    assert root.getRowCount() == ROW_COUNT;
    FlightWriter writer = new FlightWriter("mock_udf_path");
    writer.sendData(root);
  }

  @Test
  public void FlightRead() {
    FlightReader reader = new FlightReader("mock_udf_path");
    reader.read();
  }

  @Test
  public void initDirectDataAndParam() {
    int i,j;
    posParams = new ArrayList<>();
    kvargs = new HashMap<>();

    data.add(new ArrayList<>(Collections.nCopies(COL_COUNT, "COL")));
    data.add(new ArrayList<>(Collections.nCopies(COL_COUNT, "INTEGER")));

    List<org.apache.arrow.vector.types.pojo.Field> arrowFieldList = new ArrayList<>();
    RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    List<FieldVector> vectors = new ArrayList<>();
//    try {
    for (i = 0; i < COL_COUNT; i++) {
      IntVector vector = new IntVector("COL" + i, allocator);
      vectors.add(vector);
      posParams.add(new ArrayList<>(Arrays.asList(0, "COL" + i)));
      arrowFieldList.add(new org.apache.arrow.vector.types.pojo.Field("COL" + i, FieldType.nullable(new ArrowType.Int(32, true)), null));
    }
    for (i = 0; i < ROW_COUNT; i++) {
      Object[] row = new Object[COL_COUNT];
      List<Object> rowData = new ArrayList<>();
      for (j = 0; j < COL_COUNT; j++) {
        row[j] = new Random().nextInt();
        rowData.add(row[j]);
        ((IntVector) vectors.get(j)).setSafe(i, (Integer) row[j]);
      }
      data.add(rowData);
      if ((i + 1) % 100000 == 0) {
        LOGGER.info("{}0 0000 rows generated.", (i + 1) / 100000);
      }
    }
    for (i = 0; i < COL_COUNT; i++) {
      vectors.get(i).setValueCount(ROW_COUNT);
    }
//    } finally {
//    allocator.close();
//      LOGGER.info(arrowFieldList.toString());
//    }
    root = new VectorSchemaRoot(arrowFieldList, vectors);

  }

  private void initRoot() {
    int i,j;
    LOGGER.info("initiating root...");

    List<org.apache.arrow.vector.types.pojo.Field> arrowFieldList = new ArrayList<>();
    RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    List<FieldVector> vectors = new ArrayList<>();
//    try {
    for (i = 0; i < COL_COUNT; i++) {
      IntVector vector = new IntVector("COL" + i, allocator);
      vectors.add(vector);
      posParams.add(new ArrayList<>(Arrays.asList(0, "COL" + i)));
      arrowFieldList.add(new org.apache.arrow.vector.types.pojo.Field("COL" + i, FieldType.nullable(new ArrowType.Int(32, true)), null));
    }
    for (i = 0; i < ROW_COUNT; i++) {
      for (j = 0; j < COL_COUNT; j++) {
        ((IntVector) vectors.get(j)).setSafe(i, new Random().nextInt());
      }
//      if ((i + 1) % 100000 == 0) {
//        LOGGER.info("{}0 0000 rows generated.", (i + 1) / 100000);
//      }
    }
    for (i = 0; i < COL_COUNT; i++) {
      vectors.get(i).setValueCount(ROW_COUNT);
    }
    root = new VectorSchemaRoot(arrowFieldList, vectors);
  }

//  @Test
  public void initTableAndParam() {
    int i, j;
    List<Field> fields = new ArrayList<>();
    List<Pair<Integer, Object>> paths = new ArrayList<>();
    for (i = 0; i<COL_COUNT;i++) {
      fields.add(new Field("col_" + i, DataType.INTEGER));
      paths.add(new Pair<>(0, "col_" + i));
    }
    Header header = new Header(Field.KEY, fields);
    BatchData batchData = new BatchData(header);
    List<Row> rows = new ArrayList<>();
    for (i=0;i<ROW_COUNT;i++) {
      Object[] row = new Object[COL_COUNT];
      for (j=0;j<COL_COUNT;j++) {
        row[j] = new Random().nextInt();
      }
      Row rowRow = new Row(header, row);
      rows.add(rowRow);
      batchData.appendRow(rowRow);
    }
    root = batchData.wrapAsVectorSchemaRoot();
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
