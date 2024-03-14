package cn.edu.tsinghua.iginx.engine.shared.function.udf.arrow;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.transform.data.BatchData;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;

import java.io.ByteArrayOutputStream;

import org.apache.arrow.vector.util.TransferPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class FlightWriter {
  private static final Logger LOGGER = LoggerFactory.getLogger(FlightWriter.class);

  private static final int port = 33333;

  private static final Location location = Location.forGrpcInsecure("localhost", port);

  private static final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

  private static UDFDataProducer producer;

  private static FlightServer flightServer;

  private static FlightClient flightClient;

  // row per batch
  private static final int defaultChunkSize = 64000;

  // storage size per batch(unused)
  private static final int defaultChunkMemorySize = 1024 * 1024 + 512 * 1024;

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private static final String PYTHON_PATH = String.join(File.separator, config.getDefaultUDFDir(), "python_scripts");

  private static String FDPath;

  public FlightWriter(String path) {
    try {
//      producer = new UDFDataProducer(allocator, location);
//      flightServer = FlightServer.builder(allocator, location, producer).build();
//      flightServer.start();
//      LOGGER.info("Server (Location): Listening on port {}", flightServer.getPort());
      flightClient = FlightClient.builder(allocator, location).build();
      LOGGER.info("Client (Location): Connected to {}", location.getUri());
      FDPath = path;
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static int getPort() {
    return port;
  }

  public void shutdown() throws InterruptedException {
    // TODO: return client to pool
//    if (flightServer != null) {
//      flightServer.shutdown();
//    }
    if (flightClient == null) {
      flightClient.close();
    }
  }

  private static int decideBatchRow(VectorSchemaRoot root, int targetChunkSize) {
    int sampleSize = 1;
    VectorSchemaRoot sampleData = root.slice(0, sampleSize);

    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    try (ArrowStreamWriter writer = new ArrowStreamWriter(sampleData, null, Channels.newChannel(outStream))) {
      writer.start();
      writer.writeBatch();
    } catch (IOException e) {
      // TODO: use iginx exception, can't interrupt the data transfer process
      // return DEFAULT_CHUNK_SIZE;
      throw new RuntimeException(e);
    } finally {
      sampleData.close();
      allocator.close();
    }
    long sampleDataSize = outStream.size();
    double averageRowSize = (double) sampleDataSize / sampleSize;
    return (int) (targetChunkSize / averageRowSize);
  }

  private FlightClient.ClientStreamListener sendBatch(VectorSchemaRoot batch) {
    FlightClient.ClientStreamListener listener = flightClient.startPut(
            FlightDescriptor.path(FDPath),
            batch, new AsyncPutListener());
    listener.putNext();
    return listener;
  }

  private FlightClient.ClientStreamListener sendBatch(VectorSchemaRoot batch, FlightClient.ClientStreamListener listener) {
    listener.putNext();
    return listener;
  }

  public void sendData(Table table) {
    sendData(table, defaultChunkMemorySize);
  }

  public void sendData(Table table, int chunkMemorySize) {
    BatchData batchData = new BatchData(table.getHeader());
    for (Row row :
            table.getRows()) {
      batchData.appendRow(row);
    }
    try (
      VectorSchemaRoot root = batchData.wrapAsVectorSchemaRoot();
    ) {
      sendData(root, chunkMemorySize);
    }
  }

  public void sendData(VectorSchemaRoot root) {
    sendData(root, defaultChunkMemorySize);
  }

  public void sendData(VectorSchemaRoot root, int chunkMemorySize) {
    long startTime, endTime;
    int rootSize = root.getRowCount(), endRow;
//    int rowsPerBatch = decideBatchRow(root, chunkMemorySize);
    int rowsPerBatch = defaultChunkSize;
    VectorSchemaRoot batch = root.slice(0,1);
    FlightClient.ClientStreamListener listener = flightClient.startPut(
            FlightDescriptor.path(FDPath),
            batch, new AsyncPutListener());
    try {
      for (int startRow = 0; startRow < rootSize; startRow += rowsPerBatch) {
        endRow = Math.min(startRow + rowsPerBatch, rootSize);
        copyVectorSchemaRoot(root.slice(startRow, endRow - startRow), batch);
//        System.out.println(batch.contentToTSVString());
        listener.putNext();
      }
//    } catch (InterruptedException e) {
//      throw new RuntimeException(e);
    } finally {
      listener.completed();
      listener.getResult();
    }

//    FlightInfo flightInfo = flightClient.getInfo(FlightDescriptor.path(FDPath));
//    System.out.println("C3: Client (Get Metadata): " + flightInfo);
//
//    // Get data information
//    try(FlightStream flightStream = flightClient.getStream(flightInfo.getEndpoints().get(0).getTicket())) {
//      int batchCount = 0;
//      try (VectorSchemaRoot vectorSchemaRootReceived = flightStream.getRoot()) {
//        System.out.println("C4: Client (Get Stream):");
//        while (flightStream.next()) {
//          batchCount++;
//          System.out.println("Client Received batchCount #" + batchCount + ", Data:");
//          System.out.print(vectorSchemaRootReceived.contentToTSVString());
//        }
//      }
//    } catch (Exception e) {
//      e.printStackTrace();
//    }

  }

  public static void copyVectorSchemaRoot(VectorSchemaRoot src, VectorSchemaRoot dst) {
    // 确保源和目标 VectorSchemaRoot 具有相同的模式
    if (!src.getSchema().equals(dst.getSchema())) {
      throw new IllegalArgumentException("Source and destination VectorSchemaRoot must have the same schema.");
    }

    for (int i = 0; i < src.getFieldVectors().size(); i++) {
      FieldVector srcVector = src.getVector(i);
      FieldVector dstVector = dst.getVector(i);

      dstVector.clear();
      dstVector.allocateNew();

      TransferPair transferPair = srcVector.makeTransferPair(dstVector);
      transferPair.transfer();
    }
    dst.setRowCount(src.getRowCount());
  }

  public void clearData() {
    Iterator<Result> deleteActionResult = flightClient.doAction(new Action("DELETE",
            FlightDescriptor.path(FDPath).getPath().get(0).getBytes(StandardCharsets.UTF_8)));
    while (deleteActionResult.hasNext()) {
      Result result = deleteActionResult.next();
      LOGGER.info("Client (Do Delete Action): " +
              new String(result.getBody(), StandardCharsets.UTF_8));
    }
  }
}
