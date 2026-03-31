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

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple
import threading
import time

import pandas as pd


"""
Base UDF Wrapper Class.

负责统一管理 UDF 元信息、生命周期钩子、运行期监控与资源约束。
__cls: 用户实现的 UDF 类
_wrapped: 用户 UDF 实例
"""


class UDFWrapper(ABC):
    """
    UDF 基类 / 包装器。

    - 通过元信息字典保存静态标识、语义与行为、运行状态与资源约束；
    - 暴露统一的生命周期方法（before_run/after_run）与监控接口（record_metrics 等）；
    - 通过 transform(data, *args, **kwargs) 抽象方法承载核心数据处理逻辑。
    """

    def __init__(self, cls, *, meta: Optional[Dict[str, Any]] = None) -> None:
        # 用户自定义 UDF 类
        self.__cls = cls

        # 用于多线程并发下的资源与统计更新
        self._lock = threading.Lock()

        # 一、静态标识信息
        self.static_info: Dict[str, Any] = {
            "name": getattr(cls, "__name__", None),
            "version": getattr(cls, "__version__", "v1"),
            "project": None,
            "namespace": None,
            "registered_at": None,
            "last_modified_at": None,
            "script_path": getattr(cls, "__file__", None),
            "developer": getattr(cls, "__author__", None),
        }

        # 二、行为与语义信息
        self.behavior_info: Dict[str, Any] = {
            "function_type": getattr(cls, "__udf_type__", None),
            "input_signature": getattr(cls, "__input_signature__", None),
            "output_signature": getattr(cls, "__output_signature__", None),
            "expected_input_scale": None,
            "tags": set(),  # 例如 {"实验", "生产", "高风险"}
            "biz_tags": set(),  # 业务自定义标签
        }

        # 三、运行期状态信息
        self.runtime_stats: Dict[str, Any] = {
            "total_calls": 0,
            "success_calls": 0,
            "failed_calls": 0,
            "last_status": None,  # "success" / "failed"
            "last_error_type": None,
            "latency_avg_ms": 0.0,
            "latency_p95_ms": 0.0,
            "latency_p99_ms": 0.0,
            "recent_latencies_ms": [],  # 简单滑窗估计分位数
            "last_called_at": None,
        }

        # 四、资源约束信息
        self.resource_constraints: Dict[str, Any] = {
            "cpu_quota": None,  # 逻辑 CPU 配额
            "memory_limit_mb": None,
            "max_concurrency": None,
            "allowed_io_types": set(),  # 例如 {"network", "disk", "filesystem"}
            "external_resource_whitelist": set(),
        }

        # 当前占用的并发度
        self._current_concurrency = 0

        # 允许外部通过 meta 覆盖 / 补充默认元信息
        if meta:
            self._merge_meta(meta)

    # ------------------------------------------------------------------
    # 基本包装行为：实例化用户 UDF，并将属性访问透传给内部实现
    # ------------------------------------------------------------------
    def __call__(self, *args, **kwargs):
        self._wrapped = self.__cls(*args, **kwargs)
        return self

    def __getattr__(self, item):
        # 未在 wrapper 上定义的属性，回退到用户实现
        return getattr(self._wrapped, item)

    # ------------------------------------------------------------------
    # 元信息合并
    # ------------------------------------------------------------------
    def _merge_meta(self, meta: Dict[str, Any]) -> None:
        """
        将外部传入的 meta 合并到四类元信息字典中。
        """
        for key, value in meta.items():
            if key in ("static_info", "behavior_info", "runtime_stats", "resource_constraints"):
                # 允许直接覆盖某一大类配置
                getattr(self, key).update(value or {})
            else:
                # 其他键统一落入 behavior_info 的自定义标签空间
                self.behavior_info[key] = value

    # ------------------------------------------------------------------
    # 生命周期钩子与调度入口
    # ------------------------------------------------------------------
    def before_run(self, run_context: Optional[Dict[str, Any]] = None) -> None:
        """
        每次调用前的钩子。可在此完成参数校验、上下文初始化与运行状态预标记。
        """
        # 默认实现只记录开始时间，具体校验逻辑交由子类重载
        now = time.time()
        with self._lock:
            self.runtime_stats["last_called_at"] = now

    def after_run(
        self,
        run_context: Optional[Dict[str, Any]] = None,
        error: Optional[BaseException] = None,
        latency_ms: Optional[float] = None,
    ) -> None:
        """
        每次调用完成后的钩子。负责更新状态标记等收尾工作。
        """
        with self._lock:
            if error is None:
                self.runtime_stats["last_status"] = "success"
                self.runtime_stats["success_calls"] += 1
            else:
                self.runtime_stats["last_status"] = "failed"
                self.runtime_stats["failed_calls"] += 1
                self.runtime_stats["last_error_type"] = type(error).__name__

            if latency_ms is not None:
                self._update_latency_stats(latency_ms)

    def record_metrics(
        self,
        run_context: Optional[Dict[str, Any]] = None,
        *,
        latency_ms: Optional[float] = None,
        error: Optional[BaseException] = None,
    ) -> None:
        """
        将本次执行过程中的关键指标写回元信息（可对接外部监控系统）。
        """
        with self._lock:
            self.runtime_stats["total_calls"] += 1
        self.after_run(run_context, error=error, latency_ms=latency_ms)

    # ------------------------------------------------------------------
    # 资源配额与并发控制
    # ------------------------------------------------------------------
    def check_resource_quota(self) -> None:
        """
        检查是否仍有可用配额；如不足应抛出异常以触发上层限流。
        """
        with self._lock:
            max_concurrency = self.resource_constraints.get("max_concurrency")
            if max_concurrency is not None and self._current_concurrency >= max_concurrency:
                raise RuntimeError("UDF concurrency quota exceeded")

    def acquire_quota(self) -> None:
        """
        获取一次执行所需的并发配额。
        """
        with self._lock:
            self.check_resource_quota()
            self._current_concurrency += 1

    def release_quota(self) -> None:
        """
        释放一次执行占用的并发配额。
        """
        with self._lock:
            if self._current_concurrency > 0:
                self._current_concurrency -= 1

    # ------------------------------------------------------------------
    # Profile 与调试信息上报（此处仅保留接口，具体实现交给上层系统）
    # ------------------------------------------------------------------
    def report_profile(self, profile: Dict[str, Any]) -> None:
        """
        上报本次执行的关键路径 profile 结果。
        具体如何对接监控系统由集成方实现或在子类中重写。
        """
        # 默认实现只是将信息保存在 behavior_info 中，便于后续调试或诊断。
        self.behavior_info["last_profile"] = profile

    def dump_debug_info(self, info: Dict[str, Any]) -> None:
        """
        上报调试信息，如关键中间结果、异常栈等。
        """
        self.behavior_info["last_debug_info"] = info

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

        # print("1----base")
        # print(result)
        # print("2----base")
        return result

    @staticmethod
    def _convert_value(v):
        if pd.isna(v):
            return None
        if hasattr(v, "item"):
            return v.item()
        return v

    @staticmethod
    def _unpack_java_params(args: tuple):
        """
        Java 通过 Pemja 调用 transform(data, args, kvargs) 时，
        args 和 kvargs 分别作为第二、第三个位置参数传入。
        本方法将它们展开为 Python 原生参数形式供用户 eval 使用。
        """
        java_args = args[0] if len(args) > 0 and isinstance(args[0], (list, tuple)) else []
        java_kwargs = args[1] if len(args) > 1 and isinstance(args[1], dict) else {}
        if java_args is None:
            java_args = []
        if java_kwargs is None:
            java_kwargs = {}
        return list(java_args), dict(java_kwargs)

    # ------------------------------------------------------------------
    # 核心抽象方法：由子类实现具体的数据处理逻辑
    # ------------------------------------------------------------------
    @abstractmethod
    def transform(self, data, *args, **kwargs):
        """
        核心数据处理逻辑，由具体 UDF/UDTF/UDAF/UDSF 子类实现。
        该方法将被 Java 层或调度器统一调用。
        data 参数为 IGinX 二维列表，子类负责转为 DataFrame 后调用用户 eval。
        """
        raise NotImplementedError

    # ------------------------------------------------------------------
    # 可选统一入口：封装一次完整的“调度 + 生命周期 + 监控”流程
    # ------------------------------------------------------------------
    def run(self, data, *args, **kwargs):
        """
        统一的执行入口。上层可以选择调用该方法以获得完整的
        before_run / transform / record_metrics / after_run / quota 管理流程。
        """
        start = time.time()
        self.check_resource_quota()
        self.acquire_quota()
        run_context: Dict[str, Any] = {
            "args": args,
            "kwargs": kwargs,
        }
        self.before_run(run_context)
        error: Optional[BaseException] = None
        try:
            result = self.transform(data, *args, **kwargs)
            return result
        except BaseException as ex:  # noqa: BLE001
            error = ex
            raise
        finally:
            end = time.time()
            latency_ms = (end - start) * 1000.0
            self.record_metrics(run_context, latency_ms=latency_ms, error=error)
            self.release_quota()

    # ------------------------------------------------------------------
    # 内部工具：更新延迟统计（简单滑窗 + 近似分位数）
    # ------------------------------------------------------------------
    def _update_latency_stats(self, latency_ms: float, window_size: int = 128) -> None:
        with self._lock:
            recent = self.runtime_stats.get("recent_latencies_ms", [])
            recent.append(latency_ms)
            if len(recent) > window_size:
                recent = recent[-window_size:]
            self.runtime_stats["recent_latencies_ms"] = recent

            # 更新平均值和近似分位数
            self.runtime_stats["latency_avg_ms"] = sum(recent) / len(recent)
            sorted_lat = sorted(recent)
            self.runtime_stats["latency_p95_ms"] = self._percentile(sorted_lat, 95)
            self.runtime_stats["latency_p99_ms"] = self._percentile(sorted_lat, 99)

    @staticmethod
    def _percentile(sorted_values, p: int) -> float:
        if not sorted_values:
            return 0.0
        k = (len(sorted_values) - 1) * (p / 100.0)
        f = int(k)
        c = min(f + 1, len(sorted_values) - 1)
        if f == c:
            return float(sorted_values[int(k)])
        d0 = sorted_values[f] * (c - k)
        d1 = sorted_values[c] * (k - f)
        return float(d0 + d1)

