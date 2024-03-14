package cn.edu.tsinghua.iginx.engine.shared.function.udf.arrow;

import java.util.List;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataSet implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataSet.class);

  private List<ArrowRecordBatch> batches;

  private Schema schema;

  private long rowCount;

  public DataSet(List<ArrowRecordBatch> batches, Schema schema, long rowCount) {
    this.batches = batches;
    this.schema = schema;
    this.rowCount = rowCount;
  }

  public List<ArrowRecordBatch> getBatches() {
    return batches;
  }

  public Schema getSchema() {
    return schema;
  }

  public long getRowCount() {
    return rowCount;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(batches);
  }
}
