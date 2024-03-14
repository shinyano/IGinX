package cn.edu.tsinghua.iginx.engine.shared.function.udf.arrow;

import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class UDFDataProducer extends NoOpFlightProducer implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(UDFDataProducer.class);

  private final BufferAllocator allocator;
  private final Location location;
  private final ConcurrentHashMap<FlightDescriptor, DataSet> datasets;
  public UDFDataProducer(BufferAllocator allocator, Location location) {
    this.allocator = allocator;
    this.location = location;
    this.datasets = new ConcurrentHashMap<>();
  }
  @Override
  public Runnable acceptPut(CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
    List<ArrowRecordBatch> batches = new ArrayList<>();
    return () -> {
      long rows = 0;
      VectorUnloader unloader;
      while (flightStream.next()) {
        unloader = new VectorUnloader(flightStream.getRoot());
        final ArrowRecordBatch arb = unloader.getRecordBatch();
        batches.add(arb);
        rows += flightStream.getRoot().getRowCount();
      }
      DataSet dataset = new DataSet(batches, flightStream.getSchema(), rows);
      datasets.put(flightStream.getDescriptor(), dataset);
      ackStream.onCompleted();
    };
  }

  @Override
  public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
    FlightDescriptor flightDescriptor = FlightDescriptor.path(
            new String(ticket.getBytes(), StandardCharsets.UTF_8));
    DataSet dataset = this.datasets.get(flightDescriptor);
    if (dataset == null) {
      throw CallStatus.NOT_FOUND.withDescription("Unknown descriptor").toRuntimeException();
    }
    try (VectorSchemaRoot root = VectorSchemaRoot.create(
            this.datasets.get(flightDescriptor).getSchema(), allocator)) {
      VectorLoader loader = new VectorLoader(root);
      listener.start(root);
      for (ArrowRecordBatch arrowRecordBatch : this.datasets.get(flightDescriptor).getBatches()) {
        loader.load(arrowRecordBatch);
        listener.putNext();
      }
      listener.completed();
    }
  }

  @Override
  public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
    FlightDescriptor flightDescriptor = FlightDescriptor.path(
            new String(action.getBody(), StandardCharsets.UTF_8));
    switch (action.getType()) {
      case "DELETE": {
        DataSet removed = datasets.remove(flightDescriptor);
        if (removed != null) {
          try {
            removed.close();
          } catch (Exception e) {
            listener.onError(CallStatus.INTERNAL
                    .withDescription(e.toString())
                    .toRuntimeException());
            return;
          }
          Result result = new Result("Delete completed".getBytes(StandardCharsets.UTF_8));
          listener.onNext(result);
        } else {
          Result result = new Result("Delete not completed. Reason: Key did not exist."
                  .getBytes(StandardCharsets.UTF_8));
          listener.onNext(result);
        }
        listener.onCompleted();
      }
    }
  }

  @Override
  public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
    FlightEndpoint flightEndpoint = new FlightEndpoint(
            new Ticket(descriptor.getPath().get(0).getBytes(StandardCharsets.UTF_8)), location);
    return new FlightInfo(
            datasets.get(descriptor).getSchema(),
            descriptor,
            Collections.singletonList(flightEndpoint),
            /*bytes=*/-1,
            datasets.get(descriptor).getRowCount()
    );
  }

  @Override
  public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
    datasets.forEach((k, v) -> { listener.onNext(getFlightInfo(null, k)); });
    listener.onCompleted();
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(datasets.values());
  }
}
