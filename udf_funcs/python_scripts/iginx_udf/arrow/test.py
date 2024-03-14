import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.flight


def get_table(nrows: int):
    return pa.table({
        "x": np.random.random(nrows),
        "y": np.random.random(nrows),
    })

def get_data():

    location = "grpc+tcp://localhost:33333"
    flight_client = pa.flight.connect(location)
    list(flight_client.do_action("clear"))
    descriptor = pa.flight.FlightDescriptor.for_path("mock_udf_path")
    table = get_table(20)
    writer, _ = flight_client.do_put(descriptor, table.schema)
    writer.write_table(table, max_chunksize=64_000)
    writer.close()

    # for flight in flight_client.list_flights():
    #     descriptor = flight.descriptor
    #     print(
    #         "Path:",
    #         descriptor.path[0].decode("utf-8"),
    #         "Rows:",
    #         flight.total_records,
    #         "Size:",
    #         flight.total_bytes,
    #     )
    #     print("=== Schema ===")
    #     print(flight.schema)
    #     print("==============")
    #     print("")
    #
    # descriptor = pa.flight.FlightDescriptor.for_path("mock_udf_path")
    # flight_into = flight_client.get_flight_info(descriptor)
    # reader = flight_client.do_get(flight_into.endpoints[0].ticket)
    # # 初始化一个空的 DataFrame
    # df = pd.DataFrame()
    #
    # for chunk in reader:
    #     # 将每个 RecordBatch 转换为 Pandas DataFrame
    #     if chunk.data is not None:
    #         print(chunk.data)
    #         # chunk_df = chunk.data.to_pandas()
    #         # # 将转换后的 DataFrame 追加到最终的 DataFrame 中
    #         # df = pd.concat([df, chunk_df], ignore_index=True)
    #
    # # print(pd)



if __name__ == '__main__':
    get_data()