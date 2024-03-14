import time
from abc import ABC

import pandas as pd

from .udf import UDF, get_constants
# from ..arrow.flight_server import get_pool
from ..utils.dataframe import list_to_PandasDF, pandasDF_to_list
import pyarrow.flight as flight
import pyarrow.compute as pc
import os


def retrieve_from_flight(client, path):
    print(f'reading data from path: {path}')
    descriptor = flight.FlightDescriptor.for_path(path)
    flight_info = client.get_flight_info(descriptor)
    print("flight_info:\n", flight_info)
    reader = client.do_get(flight_info.endpoints[0].ticket)
    print("reader:\n", reader)
    # 初始化一个空的 DataFrame
    df = pd.DataFrame()

    for chunk in reader:
        # 将每个 RecordBatch 转换为 Pandas DataFrame
        chunk_df = chunk.to_pandas()
        # 将转换后的 DataFrame 追加到最终的 DataFrame 中
        df = pd.concat([df, chunk_df], ignore_index=True)

    return df


class UDSF(UDF, ABC):
    """
    使用dataframe模式，set to set应当只允许dataframe模式
    """

    @property
    def udf_type(self):
        return "UDSF"

    def build_header(self, paths, types):
        # 用户直接返回dataframe，这个函数应当不需要
        pass

    def transform(self, data, pos_args, kwargs):
        df = list_to_PandasDF(data)
        # 直接作为完整的dataframe进行处理，返回值为dataframe
        res = self.eval(df, *get_constants(pos_args), **kwargs)
        return pandasDF_to_list(res)

    def flight_transform(self, path, pos_args, kwargs):
        try:
            # server_location = "grpc+tcp://localhost:33333"
            # pool = FlightClientPool(server_location, pool_size=5)
            #
            # print("pool created")
            #
            # # 从池中获取客户端并获取信息
            # res = pool.execute(retrieve_from_flight, path)
            # print(res)
            client = flight.connect('grpc://localhost:33333')
            # client = get_pool().get_client()
            print(f'reading data from path: {path}')

            descriptor = flight.FlightDescriptor.for_path(path)
            # descriptor = flight.FlightDescriptor.for_path("mock_udf_path")
            flight_info = client.get_flight_info(descriptor)
            # print("flight_info:\n", flight_info)
            start_time = time.time()
            reader = client.do_get(flight_info.endpoints[0].ticket)
            df = pd.DataFrame(data=[], columns=[])
            for chunk in reader:
                # for col in chunk.data.columns:
                #     pc.sum(col)
                if chunk.data is not None:
                    #  转 Pandas DataFrame
                    chunk_df = chunk.data.to_pandas()
                #     df = pd.concat([df, chunk_df], ignore_index=True)
            end_time = time.time()
            print(f"data read time(including to_pandas()): {end_time - start_time}")
            print(list(df))
            print(len(df))
            # get_pool().return_client()
            return [[end_time - start_time]]
        except Exception as e:
            return [[str(e)]]

        # # 请求数据流
        # reader = client.do_get(descriptor)
        #
        # # 读取并打印接收到的数据
        # # 注意：这里仅作为展示，实际应用中你可能需要对数据进行进一步的处理
        # for batch in reader:
        #     print(batch)
