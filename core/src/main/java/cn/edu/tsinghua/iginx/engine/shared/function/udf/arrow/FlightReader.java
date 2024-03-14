package cn.edu.tsinghua.iginx.engine.shared.function.udf.arrow;

import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlightReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(FlightReader.class);

  private static String FDPath;

  private static Location location = Location.forGrpcInsecure("localhost", 33333);


  public FlightReader(String path) {
    FDPath = path;
  }

  public void read() {
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    FlightClient flightClient = FlightClient.builder(allocator, location).build();
    Iterable<FlightInfo> flights = flightClient.listFlights(Criteria.ALL);
    flights.forEach(flight -> System.out.println(flight.getDescriptor().getPath()));

    FlightInfo flightInfo = flightClient.getInfo(FlightDescriptor.path(FDPath));
    System.out.println("C3: Client (Get Metadata): " + flightInfo);

    // Get data information
    try(FlightStream flightStream = flightClient.getStream(flightInfo.getEndpoints().get(0).getTicket())) {
      int batchCount = 0;
      try (VectorSchemaRoot vectorSchemaRootReceived = flightStream.getRoot()) {
        System.out.println("C4: Client (Get Stream):");
        while (flightStream.next()) {
          batchCount++;
          LOGGER.info("Client Received batchCount #" + batchCount + ", RowCount:" + vectorSchemaRootReceived.getRowCount());
          LOGGER.info("Data schema: " + flightStream.getSchema());
//          System.out.print(vectorSchemaRootReceived.contentToTSVString());
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
