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
import jvm
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

    def read_data(self, root):
        # print(root)
        # print(root.getSchema())
        start = time.time()
        rb = jvm.record_batch(root)
        end = time.time()
        print(f"[PYTHON] read arrow data took {(end - start)*1000}ms")
        start = time.time()
        df =  rb.to_pandas()
        end = time.time()
        print(f"[PYTHON] arrow2pandas took {(end - start)*1000}ms")
        return df
        # print(rb.columns)

    def read_data_import(self) -> pd.DataFrame:
        if self.__array_address is None:
            raise RuntimeError("Address not initialized")

        print(f"reading data from {self.__array_address}")
        with pa.RecordBatchReader._import_from_c(self.__array_address) as source:
            print("Data read:")
            res = source.read_all()
            df = res.to_pandas()
            # print(df)
        return df

    def transform(self,data, arg, kvargs):
        self.c_stream = arrow_c.new("struct ArrowArrayStream*")
        c_stream_ptr = int(arrow_c.cast("uintptr_t", self.c_stream))


        all_start = time.time()
        start = all_start
        df = self.read_data(data)
        end = time.time()
        print(f"[PYTHON] reading df took {(end - start)*1000}ms")
        # print(f"df:{df}")
        start = time.time()
        batch = pa.RecordBatch.from_pandas(self.eval(df))
        end = time.time()
        print(f"[PYTHON] computing df took {(end - start)*1000}ms")
        # print(f"batch:{batch}")
        start = time.time()
        reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
        start = time.time()
        reader._export_to_c(c_stream_ptr)
        end = time.time()
        print(f"[PYTHON] exporting results took {(end - start)*1000}ms")
        return c_stream_ptr
        # 在这里将batch传回给__array_address

    def eval(self, data : pd.DataFrame, weight_a=1, weight_b=1) -> pd.DataFrame:
        # 使用itertuples遍历DataFrame的每一行
        if 'key' in list(data):
            data = data.drop(columns=['key'])
        def custom_min(col):
            # 过滤掉None值
            valid_values = col.dropna()
            if valid_values.empty:
                return None
            return valid_values.min()

        # 应用自定义最小值函数到每一列
        mins = data.min()
        # print("minnnnnnnnnnnnnnnnnnnnnnnnnn")
        # print(mins)
        # print("minnnnnnnnnnnnnnnnnnnnnnnnnn")
        # print(mins.to_frame().T)
        # print("minnnnnnnnnnnnnnnnnnnnnnnnnn")

        return mins.to_frame().T

def test_calculate_column_mins():
    from pandas.testing import assert_series_equal
    t = UDAFinDFTest()
    # 测试用例 1: 正常情况
    df1 = pd.DataFrame({
        'A': [1,2,3],
        'B': ['4', '5', '6'],
        'C': ['7', '8', '9']
    })
    expected1 = pd.Series({'A': 1, 'B': '4', 'C': '7'})
    result1 = t.eval(df1)
    assert_series_equal(result1, expected1)
    print("Test case 1 passed!")

    # 测试用例 2: 包含None值
    df2 = pd.DataFrame({
        'A': ['1', None, '3'],
        'B': [None, '5', '6'],
        'C': ['7', '8', None]
    })
    expected2 = pd.Series({'A': '1', 'B': '5', 'C': '7'})
    result2 = t.eval(df2)
    assert_series_equal(result2, expected2)
    print("Test case 2 passed!")

    # 测试用例 3: 全部为None的列
    df3 = pd.DataFrame({
        'A': ['1', '2', '3'],
        'B': [None, None, None],
        'C': ['7', '8', '9']
    })
    expected3 = pd.Series({'A': '1', 'B': None, 'C': '7'})
    result3 = t.eval(df3)
    assert_series_equal(result3, expected3)
    print("Test case 3 passed!")



    # 测试用例 4: 数值比较（字符串形式）
    df4 = pd.DataFrame({
        'A': ['10', '2', '30'],
        'B': ['4', '50', '6'],
        'C': ['007', '8', '9']
    })
    expected4 = pd.Series({'A': '10', 'B': '4', 'C': '007'})
    result4 = t.eval(df4)
    assert_series_equal(result4, expected4)
    print("Test case 4 passed!")

    print("All test cases passed successfully!")

# 运行测试
if __name__ == "__main__":
    test_calculate_column_mins()



"""
df2 = pd.DataFrame(columns=["udaf_test(test.a, test.b)"])

from udaf_df_test import UDAFinDFTest
data = [["key","a","b"],["LONG","INTEGER","INTEGER"],[0,1,2],[1,2,3],[2,3,4]]
pos_args = [[0,"a"],[0,"b"],[1,2],[1,3]]
kwargs = {}
test = UDAFinDFTest()
test.transform(data,pos_args,kwargs)

from udf_max import UDFMax
data = [["key","a","b"],["LONG","INTEGER","INTEGER"],[0,1,2],[1,2,3],[2,3,4]]
pos_args = [[0,"a"],[0,"b"]]
kwargs = {}
test = UDFMax()
test.udf_name = 'max'
test.transform(data,pos_args,kwargs)

insert into test(key,a,b) values(0,1,2),(1,2,3);
REGISTER UDAF PYTHON TASK "UDAFinDFTest" IN "udf_funcs\\python_scripts\\udaf_df.py" AS "udaf_df_test";

select udaf_df_test(a,b) from test;
select udaf_df_test(a,b,2) from test;
select udaf_df_test(a,b,2,3) from test;
select udaf_df_test(a,b,weight_a=2) from test;
select udaf_df_test(a,b,weight_b=3) from test;
select udaf_df_test(a,b,weight_a=2, weight_b=3) from test;
"""
