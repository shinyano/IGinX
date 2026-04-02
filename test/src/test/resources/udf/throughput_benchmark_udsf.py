import sys

import numpy as np
import pandas as pd

from iginx_udf import UDSFWrapper


def _normalize_loops(loops):
    if isinstance(loops, (bytes, bytearray)):
        loops = loops.decode()
    if isinstance(loops, str):
        loops = loops.strip()
    if isinstance(loops, float):
        loops = int(loops)
    return int(loops)


def _value_rows(data):
    value_columns = [column for column in data.columns if column != "key"]
    return [list(map(float, row)) for row in data[value_columns].itertuples(index=False, name=None)]


@UDSFWrapper
class BenchmarkMetadata:
    def eval(self, data, *args, **kwargs):
        version = sys.version_info
        gil_enabled = int(getattr(sys, "_is_gil_enabled", lambda: True)())
        return pd.DataFrame(
            {
                "gil_enabled": [gil_enabled],
                "py_major": [version.major],
                "py_minor": [version.minor],
                "py_micro": [version.micro],
            }
        )


@UDSFWrapper
class ComputeIntensiveBenchmark:
    def eval(self, data, loops=20, *args, **kwargs):
        loops = _normalize_loops(loops)
        rows = _value_rows(data)
        total = 0.0
        for _ in range(loops):
            for row in rows:
                for value in row:
                    total += value * value
        return pd.DataFrame({"result": [total]})


@UDSFWrapper
class NumpyBenchmark:
    def eval(self, data, loops=20, *args, **kwargs):
        loops = _normalize_loops(loops)
        value_columns = [column for column in data.columns if column != "key"]
        values = data[value_columns].to_numpy(dtype=np.float64, copy=False)
        total = 0.0
        for _ in range(loops):
            total += float(np.sum(values * values))
        return pd.DataFrame({"result": [total]})
