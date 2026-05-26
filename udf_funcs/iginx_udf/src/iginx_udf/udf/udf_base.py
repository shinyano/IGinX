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

from abc import ABC
from typing import List, Optional, Tuple

import pandas as pd
try:
    import pyarrow as pa
except ImportError:  # pragma: no cover
    pa = None


class UDFWrapper(ABC):
    """
    UDF 基类 / 包装器。

    负责 IGinX 二维列表与 pandas DataFrame 的格式互转，
    并通过 transform() 抽象方法承载核心数据处理逻辑。
    """

    def __init__(self, cls) -> None:
        self.__cls = cls

    # ------------------------------------------------------------------
    # 基本包装行为：实例化用户 UDF，并将属性访问透传给内部实现
    # ------------------------------------------------------------------
    def __call__(self, *args, **kwargs):
        self._wrapped = self.__cls(*args, **kwargs)
        return self

    def __getattr__(self, item):
        return getattr(self._wrapped, item)

    # ------------------------------------------------------------------
    # 数据格式转换：IGinX 二维列表 <-> pandas DataFrame
    # ------------------------------------------------------------------
    IGINX_TO_PANDAS_DTYPE = {
        "BOOLEAN": "bool",
        "INTEGER": "int32",
        "LONG": "int64",
        "FLOAT": "float32",
        "DOUBLE": "float64",
        "BINARY": "object",
    }

    PANDAS_TO_IGINX_DTYPE = {
        "bool": "BOOLEAN",
        "int32": "INTEGER",
        "int64": "LONG",
        "float32": "FLOAT",
        "float64": "DOUBLE",
        "float16": "FLOAT",
        "object": "BINARY",
    }

    def _list_to_dataframe(self, data: list) -> Tuple[pd.DataFrame, List[str], bool]:
        """
        将 IGinX 的二维列表 [colNames, colTypes, row1, row2, ...] 转为 DataFrame。

        Returns:
            (df, original_types, has_key)
            - df: 转换后的 DataFrame（若有 key 列则包含在内）
            - original_types: 原始类型字符串列表（与 data[1] 对齐）
            - has_key: 首列是否为 key
        """
        if not data or len(data) < 2:
            return pd.DataFrame(), [], False

        col_names = list(data[0])
        col_types = list(data[1])
        rows = data[2:]

        has_key = len(col_names) > 0 and col_names[0] == "key"

        df = pd.DataFrame(rows, columns=col_names)

        for col_name, col_type in zip(col_names, col_types):
            pandas_dtype = self.IGINX_TO_PANDAS_DTYPE.get(col_type)
            if pandas_dtype and col_name in df.columns:
                try:
                    df[col_name] = df[col_name].astype(pandas_dtype)
                except (ValueError, TypeError):
                    pass

        return df, col_types, has_key

    @staticmethod
    def _infer_iginx_type(series: pd.Series) -> str:
        dtype_str = str(series.dtype)
        for pd_type, ig_type in UDFWrapper.PANDAS_TO_IGINX_DTYPE.items():
            if pd_type in dtype_str:
                return ig_type
        return "BINARY"

    def _dataframe_to_list(
        self, df: pd.DataFrame, original_types: Optional[List[str]] = None, has_key: bool = False
    ) -> list:
        """
        将 DataFrame 转回 IGinX 的二维列表格式 [colNames, colTypes, row1, row2, ...]。
        """
        if df is None or df.empty:
            return []

        col_names = list(df.columns)

        col_types = []
        for name in col_names:
            col_types.append(self._infer_iginx_type(df[name]))

        result: List[list] = [col_names, col_types]
        # itertuples() 比 iterrows() 更快，且不会把一整行强制提升为同一种 dtype。
        for row in df.itertuples(index=False, name=None):
            result.append([self._convert_value(v) for v in row])

        return result

    def _arrow_to_dataframe(self, data) -> Tuple[pd.DataFrame, int]:
        """
        将 Java 侧传入的 Arrow RecordBatch 转为 DataFrame。
        """
        self._require_pyarrow()
        if not isinstance(data, pa.RecordBatch):
            raise TypeError(
                f"arrow_transform() expects pyarrow.RecordBatch as the first argument, "
                f"got {type(data).__name__}."
            )

        return data.to_pandas(), data.num_rows

    def _dataframe_to_arrow(self, df: pd.DataFrame):
        """
        将 DataFrame 转为 Arrow RecordBatch。
        空结果统一编码为空 schema 的空 RecordBatch。
        """
        self._require_pyarrow()
        if df is None or df.empty:
            return pa.RecordBatch.from_arrays([], names=[])

        return pa.RecordBatch.from_pandas(df, preserve_index=False)

    @staticmethod
    def _convert_value(v):
        if pd.isna(v):
            return None
        if hasattr(v, "item"):
            return v.item()
        return v

    @staticmethod
    def _unpack_java_params(java_args, java_kwargs):
        """
        Java 通过 Pemja 调用 transform(data, args, kvargs) 时，
        args 和 kvargs 分别作为第二、第三个位置参数传入。
        本方法将它们展开为 Python 原生参数形式供用户 eval 使用。
        """
        if java_args is None:
            java_args = []
        elif not isinstance(java_args, (list, tuple)):
            java_args = []
        if java_kwargs is None:
            java_kwargs = {}
        elif not isinstance(java_kwargs, dict):
            java_kwargs = {}
        return list(java_args), dict(java_kwargs)

    @staticmethod
    def _require_pyarrow():
        if pa is None:
            raise ImportError(
                "pyarrow is required for arrow_transform(). "
                "Please install the 'pyarrow' package in the Python UDF environment."
            )

    def _run_eval(self, df: pd.DataFrame, args=None, kwargs=None):
        user_args, user_kwargs = self._unpack_java_params(args, kwargs)
        return self._wrapped.eval(df, *user_args, **user_kwargs)

    def _validate_result(self, result, input_rows: int):
        pass

    def _execute_list_transform(self, data, args=None, kwargs=None):
        df, original_types, has_key = self._list_to_dataframe(data)
        result = self._run_eval(df, args, kwargs)
        self._validate_result(result, len(df))
        if isinstance(result, pd.DataFrame):
            return self._dataframe_to_list(result, original_types, has_key)
        return result

    def _execute_arrow_transform(self, data, args=None, kwargs=None):
        df, input_rows = self._arrow_to_dataframe(data)
        result = self._run_eval(df, args, kwargs)
        self._validate_result(result, input_rows)
        if isinstance(result, pd.DataFrame):
            return self._dataframe_to_arrow(result)
        if pa is not None and isinstance(result, pa.RecordBatch):
            return result
        raise TypeError(
            "arrow_transform() only accepts pandas.DataFrame or pyarrow.RecordBatch "
            f"as eval() return values, got {type(result).__name__}."
        )

    def transform(self, data, args=None, kwargs=None):
        """
        旧协议入口：接收 IGinX 二维列表并返回兼容的二维列表结果。
        """
        return self._execute_list_transform(data, args, kwargs)

    # ------------------------------------------------------------------
    # 核心抽象方法：由子类实现具体的数据处理逻辑
    # ------------------------------------------------------------------
    def arrow_transform(self, data, args=None, kwargs=None):
        """
        Arrow 协议入口：接收 RecordBatch，调用用户 eval 后返回 RecordBatch。
        """
        return self._execute_arrow_transform(data, args, kwargs)
