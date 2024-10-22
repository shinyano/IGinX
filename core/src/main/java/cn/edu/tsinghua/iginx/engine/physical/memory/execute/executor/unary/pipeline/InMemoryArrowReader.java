package cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.unary.pipeline;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

public class InMemoryArrowReader extends ArrowReader {
  private final Schema schema;
  private final List<ArrowRecordBatch> batches;
  private final DictionaryProvider provider;
  private int nextBatch;

  public InMemoryArrowReader(
      BufferAllocator allocator,
      Schema schema,
      List<ArrowRecordBatch> batches,
      DictionaryProvider provider) {
    super(allocator);
    this.schema = schema;
    this.batches = batches;
    this.provider = provider;
    this.nextBatch = 0;
  }

  @Override
  public Dictionary lookup(long id) {
    return provider.lookup(id);
  }

  @Override
  public Set<Long> getDictionaryIds() {
    return provider.getDictionaryIds();
  }

  @Override
  public Map<Long, Dictionary> getDictionaryVectors() {
    return getDictionaryIds().stream().collect(Collectors.toMap(Function.identity(), this::lookup));
  }

  @Override
  public boolean loadNextBatch() throws IOException {
    if (nextBatch < batches.size()) {
      VectorLoader loader = new VectorLoader(getVectorSchemaRoot());
      loader.load(batches.get(nextBatch++));
      return true;
    }
    return false;
  }

  @Override
  public long bytesRead() {
    return 0;
  }

  @Override
  protected void closeReadSource() throws IOException {
    try {
      AutoCloseables.close(batches);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  protected Schema readSchema() {
    return schema;
  }
}
