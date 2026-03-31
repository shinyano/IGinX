#
# IGinX - the polystore system with high performance
# Copyright (C) Tsinghua University
# TSIGinX@gmail.com
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 3 of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, write to the Free Software Foundation,
# Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
#

import pandas as pd
from iginx_udf import UDTFWrapper, UDAFWrapper, UDSFWrapper

# ============================================================
# Part A: DataFrame 透明转换测试
# ============================================================

def test_udtf_dataframe_passthrough():
    """UDTF: wrapper 自动将 IGinX 二维列表转为 DataFrame，用户 eval 操作 DataFrame"""
    @UDTFWrapper
    class MyUDTF:
        def eval(self, data, *args, **kwargs):
            assert isinstance(data, pd.DataFrame), "eval should receive DataFrame"
            cols = [c for c in data.columns if c != "key"]
            result = {}
            for col in cols:
                result["double(" + col + ")"] = [data[col].iloc[0] * 2]
            return pd.DataFrame(result)

    udf = MyUDTF()
    # IGinX 格式: [colNames, colTypes, row1, ...]
    iginx_input = [
        ["key", "s1", "s2"],
        ["LONG", "LONG", "DOUBLE"],
        [0, 10, 3.14],
    ]
    result = udf.transform(iginx_input, [], {})

    assert result[0] == ["double(s1)", "double(s2)"]
    assert len(result) == 3  # header + types + 1 data row
    assert result[2][0] == 20
    assert abs(result[2][1] - 6.28) < 0.01


def test_udaf_dataframe_aggregation():
    """UDAF: 多行聚合，输入多行 DataFrame，输出单行"""
    @UDAFWrapper
    class MyUDAF:
        def eval(self, data, *args, **kwargs):
            cols = [c for c in data.columns if c != "key"]
            result = {}
            for col in cols:
                result["sum(" + col + ")"] = [data[col].sum()]
            return pd.DataFrame(result)

    udf = MyUDAF()
    iginx_input = [
        ["key", "val"],
        ["LONG", "LONG"],
        [0, 10],
        [1, 20],
        [2, 30],
    ]
    result = udf.transform(iginx_input, [], {})

    assert result[0] == ["sum(val)"]
    assert result[2][0] == 60


def test_udsf_dataframe_set_to_set():
    """UDSF: 集合映射，保持行数"""
    @UDSFWrapper
    class MyUDSF:
        def eval(self, data, *args, **kwargs):
            cols = [c for c in data.columns if c != "key"]
            result = data[["key"]].copy() if "key" in data.columns else pd.DataFrame()
            for col in cols:
                result["rev(" + col + ")"] = data[col].values[::-1]
            return result

    udf = MyUDSF()
    iginx_input = [
        ["key", "s1"],
        ["LONG", "LONG"],
        [0, 1],
        [1, 2],
        [2, 3],
    ]
    result = udf.transform(iginx_input, [], {})

    assert result[0] == ["key", "rev(s1)"]
    assert len(result) == 5  # header + types + 3 rows
    assert result[2][1] == 3  # reversed
    assert result[3][1] == 2
    assert result[4][1] == 1


# ============================================================
# Part B: _list_to_dataframe / _dataframe_to_list 直接测试
# ============================================================

def test_list_to_dataframe_with_key():
    @UDTFWrapper
    class Dummy:
        def eval(self, data, *args, **kwargs):
            return data

    wrapper = Dummy()

    data = [
        ["key", "col_a", "col_b"],
        ["LONG", "INTEGER", "DOUBLE"],
        [0, 1, 2.5],
        [1, 3, 4.5],
    ]
    df, types, has_key = wrapper._list_to_dataframe(data)

    assert has_key is True
    assert list(df.columns) == ["key", "col_a", "col_b"]
    assert len(df) == 2
    assert types == ["LONG", "INTEGER", "DOUBLE"]


def test_list_to_dataframe_without_key():
    @UDTFWrapper
    class Dummy:
        def eval(self, data, *args, **kwargs):
            return data

    wrapper = Dummy()

    data = [
        ["col_a", "col_b"],
        ["LONG", "DOUBLE"],
        [10, 20.0],
    ]
    df, types, has_key = wrapper._list_to_dataframe(data)

    assert has_key is False
    assert list(df.columns) == ["col_a", "col_b"]
    assert len(df) == 1


def test_dataframe_to_list_roundtrip():
    @UDTFWrapper
    class Dummy:
        def eval(self, data, *args, **kwargs):
            return data

    wrapper = Dummy()

    original = [
        ["key", "s1", "s2"],
        ["LONG", "LONG", "DOUBLE"],
        [0, 100, 1.5],
        [1, 200, 2.5],
    ]

    df, types, has_key = wrapper._list_to_dataframe(original)
    restored = wrapper._dataframe_to_list(df, types, has_key)

    assert restored[0] == original[0]
    assert restored[1] == original[1]
    assert len(restored) == len(original)
    for r in range(2, len(original)):
        for c in range(len(original[0])):
            orig_val = original[r][c]
            rest_val = restored[r][c]
            if isinstance(orig_val, float):
                assert abs(orig_val - rest_val) < 1e-6
            else:
                assert orig_val == rest_val


def test_dataframe_to_list_udf_avg_style_columns():
    """含 IGinX 聚合列名（如 udf_avg(us.d1.s1)）的单行 DataFrame 转二维列表"""
    @UDTFWrapper
    class Dummy:
        def eval(self, data, *args, **kwargs):
            return data

    wrapper = Dummy()
    c1 = "udf_avg(us.d1.s1)"
    c2 = "udf_avg(us.d1.s2)"

    df_a = pd.DataFrame({c1: [4.5], c2: [5.0]})
    expected_a = [
        [c1, c2],
        ["DOUBLE", "DOUBLE"],
        [4.5, 5.0],
    ]
    out_a = wrapper._dataframe_to_list(df_a)
    assert out_a[0] == expected_a[0]
    assert out_a[1] == expected_a[1]
    assert len(out_a) == 3
    assert abs(out_a[2][0] - expected_a[2][0]) < 1e-9
    assert abs(out_a[2][1] - expected_a[2][1]) < 1e-9

    df_b = pd.DataFrame({c1: [14.5], c2: [15.0]})
    expected_b = [
        [c1, c2],
        ["DOUBLE", "DOUBLE"],
        [14.5, 15.0],
    ]
    out_b = wrapper._dataframe_to_list(df_b)
    assert out_b[0] == expected_b[0]
    assert out_b[1] == expected_b[1]
    assert len(out_b) == 3
    assert abs(out_b[2][0] - expected_b[2][0]) < 1e-9
    assert abs(out_b[2][1] - expected_b[2][1]) < 1e-9


def test_empty_dataframe_to_list():
    @UDTFWrapper
    class Dummy:
        def eval(self, data, *args, **kwargs):
            return data

    wrapper = Dummy()
    result = wrapper._dataframe_to_list(pd.DataFrame())
    assert result == []


def test_list_to_dataframe_empty_input():
    @UDTFWrapper
    class Dummy:
        def eval(self, data, *args, **kwargs):
            return data

    wrapper = Dummy()
    df, types, has_key = wrapper._list_to_dataframe([])
    assert df.empty
    assert types == []
    assert has_key is False


# ============================================================
# Part C: _unpack_java_params 测试
# ============================================================

def test_unpack_java_params_normal():
    from iginx_udf.udf.udf_base import UDFWrapper
    args = ([1, 2, 3], {"n": 5, "mode": "fast"})
    user_args, user_kwargs = UDFWrapper._unpack_java_params(args)
    assert user_args == [1, 2, 3]
    assert user_kwargs == {"n": 5, "mode": "fast"}


def test_unpack_java_params_empty():
    from iginx_udf.udf.udf_base import UDFWrapper
    args = ([], {})
    user_args, user_kwargs = UDFWrapper._unpack_java_params(args)
    assert user_args == []
    assert user_kwargs == {}


def test_unpack_java_params_none_values():
    from iginx_udf.udf.udf_base import UDFWrapper
    args = (None, None)
    user_args, user_kwargs = UDFWrapper._unpack_java_params(args)
    assert user_args == []
    assert user_kwargs == {}


def test_unpack_java_params_no_args():
    from iginx_udf.udf.udf_base import UDFWrapper
    args = ()
    user_args, user_kwargs = UDFWrapper._unpack_java_params(args)
    assert user_args == []
    assert user_kwargs == {}


# ============================================================
# Part D: 参数原生绑定测试（用户 eval 直接接收 Python 参数）
# ============================================================

def test_udtf_native_kwargs():
    """验证 Java 传的 args/kvargs 被展开为 Python 原生参数"""
    @UDTFWrapper
    class PowUDF:
        def eval(self, data, n=1, **kwargs):
            cols = [c for c in data.columns if c != "key"]
            result = {}
            for col in cols:
                result["pow(" + col + ")"] = [data[col].iloc[0] ** n]
            return pd.DataFrame(result)

    udf = PowUDF()
    iginx_input = [
        ["key", "s1"],
        ["LONG", "LONG"],
        [0, 3],
    ]
    # 模拟 Java 调用: transform(data, [args_list], {kwargs_dict})
    result = udf.transform(iginx_input, [], {"n": 4})

    assert result[2][0] == 81  # 3^4


def test_udtf_native_positional_args():
    @UDTFWrapper
    class AddUDF:
        def eval(self, data, offset, *args, **kwargs):
            cols = [c for c in data.columns if c != "key"]
            result = {}
            for col in cols:
                result[col] = [data[col].iloc[0] + offset]
            return pd.DataFrame(result)

    udf = AddUDF()
    iginx_input = [
        ["key", "val"],
        ["LONG", "LONG"],
        [0, 10],
    ]
    result = udf.transform(iginx_input, [100], {})

    assert result[2][0] == 110  # 10 + 100


# ============================================================
# Part E: 端到端 wrapper 测试（模拟完整 Java 调用路径）
# ============================================================

def test_full_pipeline_udaf_avg():
    """模拟 Java 传入完整 IGinX 二维列表，验证 UDAF avg 的 DataFrame 管道"""
    @UDAFWrapper
    class AvgUDF:
        def eval(self, data, *args, **kwargs):
            cols = [c for c in data.columns if c != "key"]
            result = {}
            for col in cols:
                result["avg(" + col + ")"] = [data[col].mean()]
            return pd.DataFrame(result)

    udf = AvgUDF()
    iginx_input = [
        ["key", "temperature", "humidity"],
        ["LONG", "DOUBLE", "DOUBLE"],
        [0, 20.0, 60.0],
        [1, 22.0, 65.0],
        [2, 24.0, 70.0],
    ]
    result = udf.transform(iginx_input, [], {})

    assert result[0] == ["avg(temperature)", "avg(humidity)"]
    assert abs(result[2][0] - 22.0) < 1e-6
    assert abs(result[2][1] - 65.0) < 1e-6


if __name__ == "__main__":
    test_funcs = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    passed, failed = 0, 0
    for fn in test_funcs:
        try:
            fn()
            print(f"  PASS: {fn.__name__}")
            passed += 1
        except Exception as e:
            print(f"  FAIL: {fn.__name__} — {e}")
            failed += 1
    print(f"\nResults: {passed} passed, {failed} failed, {passed + failed} total")
