import pandas as pd
import pyarrow as pa
from pyarrow.cffi import ffi as arrow_c


def recv(addr:int) -> pd.Dataframe:
    with pa.RecordBatchReader._import_from_c(addr) as source:
        res = source.read_all()
        df = res.to_pandas(types_mapper=pd.ArrowDtype)
    return df


def send(data:pd.Dataframe) -> int:
    batch = pa.RecordBatch.from_pandas(data)
    reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    c_stream = arrow_c.new("struct ArrowArrayStream*")
    c_stream_ptr = int(arrow_c.cast("uintptr_t", c_stream))
    reader._export_to_c(c_stream_ptr)
    return c_stream_ptr

