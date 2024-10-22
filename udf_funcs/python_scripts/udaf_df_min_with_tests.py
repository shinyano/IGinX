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
            df = res.to_pandas(types_mapper=pd.ArrowDtype)
            end = time.perf_counter()
            print(f"[PYTHON] arrow2pandas took {(end - start)*1000}ms")
            # print(df)
        return df

    def transform(self, addr, arg, kvargs):
        df = self.read_data_import(addr)

        res = self.eval(df)
        start = time.perf_counter()
        batch = pa.RecordBatch.from_pandas(res)
        end = time.perf_counter()
        print(f"[PYTHON] computing res df took {(end - start)*1000}ms")
        # print("ressssssssssssssssssssss")
        # print(batch)
        # print("ressssssssssssssssssssss")
        start = time.perf_counter()
        reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
        self.c_stream = arrow_c.new("struct ArrowArrayStream*")
        c_stream_ptr = int(arrow_c.cast("uintptr_t", self.c_stream))
        reader._export_to_c(c_stream_ptr)
        end = time.perf_counter()
        print(f"[PYTHON] exporting results took {(end - start)*1000}ms")
        return c_stream_ptr
        # 在这里将batch传回给__array_address

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

def test_to_pandas(num_rows, num_iterations=100):
    import numpy as np
    col1 = np.random.randn(num_rows)
    col2 = np.random.uniform(0, 100, num_rows)
    col3 = np.random.exponential(scale=1.0, size=num_rows)
    col4 = np.random.gamma(shape=2.0, scale=2.0, size=num_rows)

    # 创建PyArrow数组
    pa_col1 = pa.array(col1, type=pa.float64())
    pa_col2 = pa.array(col2, type=pa.float64())
    pa_col3 = pa.array(col3, type=pa.float64())
    pa_col4 = pa.array(col4, type=pa.float64())

    # 创建PyArrow表格
    table = pa.Table.from_arrays(
        [pa_col1, pa_col2, pa_col3, pa_col4],
        names=['Column1', 'Column2', 'Column3', 'Column4']
    )

    total_time = 0
    for _ in range(num_iterations):
        start = time.perf_counter()
        df = table.to_pandas(types_mapper=pd.ArrowDtype)
        end = time.perf_counter()
        total_time += (end - start)
        # print(f"[PYTHON] min() took {(end - start)*1000:.6f}ms for {num_rows} rows")

    average_time = (total_time / num_iterations) * 1000  # Convert to milliseconds
    print(f"[PYTHON] to_pandas() took an average of {average_time:.6f}ms for {num_rows} rows (over {num_iterations} iterations)")


def test_min_improved(num_rows, num_iterations=100):
    import numpy as np
    import pyarrow as pa
    import pandas as pd
    import time

    col1 = np.random.randn(num_rows)
    col2 = np.random.randn(num_rows)
    col3 = np.random.randn(num_rows)
    col4 = np.random.randn(num_rows)

    pa_col1 = pa.array(col1, type=pa.float64())
    pa_col2 = pa.array(col2, type=pa.float64())
    pa_col3 = pa.array(col3, type=pa.float64())
    pa_col4 = pa.array(col4, type=pa.float64())

    table = pa.Table.from_arrays(
        [pa_col1, pa_col2, pa_col3, pa_col4],
        names=['Column1', 'Column2', 'Column3', 'Column4']
    )
    df = table.to_pandas(types_mapper=pd.ArrowDtype)

    total_time = 0
    for _ in range(num_iterations):
        start = time.perf_counter()
        mins = df.min()
        end = time.perf_counter()
        total_time += (end - start)
        # print(f"[PYTHON] min() took {(end - start)*1000:.6f}ms for {num_rows} rows")

    average_time = (total_time / num_iterations) * 1000  # Convert to milliseconds
    print(f"[PYTHON] min() took an average of {average_time:.6f}ms for {num_rows} rows (over {num_iterations} iterations)")


def test_from_pandas(num_rows, num_iterations=100):
    import numpy as np
    import pyarrow as pa
    import pandas as pd
    import time

    df = pd.DataFrame({
        'Column1': np.random.randn(num_rows),
        'Column2': np.random.uniform(0, 100, num_rows),
        'Column3': np.random.exponential(scale=1.0, size=num_rows),
        'Column4': np.random.gamma(shape=2.0, scale=2.0, size=num_rows)
    })

    # 确保所有列都是float64类型
    df = df.astype('float64')

    total_time = 0
    for _ in range(num_iterations):
        start = time.perf_counter()
        batch = pa.RecordBatch.from_pandas(df)
        end = time.perf_counter()
        total_time += (end - start)
        # print(f"[PYTHON] min() took {(end - start)*1000:.6f}ms for {num_rows} rows")

    average_time = (total_time / num_iterations) * 1000  # Convert to milliseconds
    print(f"[PYTHON] from_pandas() took an average of {average_time:.6f}ms for {num_rows} rows (over {num_iterations} iterations)")

# 运行测试
if __name__ == "__main__":
    for _ in range(10):  # Run each test 5 times
        # test_min_improved(2)
        # test_min_improved(70)
        # test_min_improved(3970)
        # test_to_pandas(2)
        # test_to_pandas(70)
        # test_to_pandas(3970)
        test_from_pandas(1)



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
