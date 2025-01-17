#
# IGinX - the polystore system with high performance
# Copyright (C) Tsinghua University
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

import pandas as pd
# from iginx_udf import UDAFinDF
from pyarrow.cffi import ffi as arrow_c
import pyarrow as pa
import time


class UDFMin():
    def __init__(self):
        self.__array_address = None
        self.c_stream = None

    def create_pointer(self):
        self.c_stream = arrow_c.new("struct ArrowArrayStream*")
        c_stream_ptr = int(arrow_c.cast("uintptr_t", self.c_stream))
        self.__array_address = c_stream_ptr
        print(f"array created at {c_stream_ptr}")
        return c_stream_ptr

    def read_data_import(self, addr) -> pd.DataFrame:
        if self.__array_address is None:
            raise RuntimeError("Address not initialized")

        print(f"reading data from {addr}")
        with pa.RecordBatchReader._import_from_c(addr) as source:
            # print("Data read:")
            start = time.perf_counter()
            res = source.read_all()
            end = time.perf_counter()
            print(f"[PYTHON] read arrow data took {(end - start)*1000}ms")
            # print(res)
            start = time.perf_counter()
            # df = res.to_pandas(types_mapper=pd.ArrowDtype)
            print(type(res))
            end = time.perf_counter()
            print(f"[PYTHON] arrow2pandas took {(end - start)*1000}ms")
            # print(df.shape)
            print(res.schema)
        return res

    def transform(self, addr, arg, kvargs):
        df = self.read_data_import(addr)
        return int(time.time() * 1000)
        #
        # res = self.eval(df)
        # start = time.perf_counter()
        # batch = pa.RecordBatch.from_pandas(res)
        # end = time.perf_counter()
        # print(f"[PYTHON] computing res df took {(end - start)*1000}ms")
        # # print("ressssssssssssssssssssss")
        # # print(batch)
        # # print("ressssssssssssssssssssss")
        # start = time.perf_counter()
        # reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
        # self.c_stream = arrow_c.new("struct ArrowArrayStream*")
        # c_stream_ptr = int(arrow_c.cast("uintptr_t", self.c_stream))
        # reader._export_to_c(c_stream_ptr)
        # end = time.perf_counter()
        # print(f"[PYTHON] exporting results took {(end - start)*1000}ms")
        # return c_stream_ptr
        # # 在这里将batch传回给__array_address

    def eval(self, data : pd.DataFrame, weight_a=1, weight_b=1) -> pd.DataFrame:
        # 使用itertuples遍历DataFrame的每一行
        if 'key' in list(data):
            data = data.drop(columns=['key'])

        # 应用自定义最小值函数到每一列
        # mins = data.apply(custom_min)
        start = time.perf_counter()
        mins = data.min()
        end = time.perf_counter()
        print(f"[PYTHON.eval] min op took {(end - start)*1000}ms")
        # print("minnnnnnnnnnnnnnnnnnnnnnnnnn")
        # print(mins)
        # print("minnnnnnnnnnnnnnnnnnnnnnnnnn")
        # print(mins.to_frame().T)
        # print("minnnnnnnnnnnnnnnnnnnnnnnnnn")

        start = time.perf_counter()
        res = mins.to_frame().T
        end = time.perf_counter()
        print(f"[PYTHON.eval] min2df took {(end - start)*1000}ms")
        return res

