import numpy as np
import pandas as pd

from iginx_udf import UDAFWrapper

MODE_COMPATIBLE_COMBO = 0
MODE_FLOAT_TO_DOUBLE = 1
MODE_NEW_COLUMN = 2
MODE_KEY_MISMATCH = 3
MODE_TYPE_CONFLICT = 4

_MODE_NAME_TO_ID = {
    "compatible_combo": MODE_COMPATIBLE_COMBO,
    "float_to_double": MODE_FLOAT_TO_DOUBLE,
    "new_column": MODE_NEW_COLUMN,
    "key_mismatch": MODE_KEY_MISMATCH,
    "type_conflict": MODE_TYPE_CONFLICT,
}


def _series(value, dtype):
    return pd.Series([value], dtype=dtype)


def _normalize_mode(mode):
    if isinstance(mode, (bytes, bytearray)):
        mode = mode.decode()
    if isinstance(mode, str):
        mode = mode.strip()
        if mode.isdigit():
            return int(mode)
        return _MODE_NAME_TO_ID.get(mode, mode)
    if isinstance(mode, float) and mode.is_integer():
        return int(mode)
    return mode


@UDAFWrapper
class SchemaGuardWindowUDAF:
    def eval(self, data, mode=MODE_COMPATIBLE_COMBO):
        mode = _normalize_mode(mode)
        value_columns = [column for column in data.columns if column != "key"]
        primary_column = value_columns[0]
        secondary_column = value_columns[1] if len(value_columns) > 1 else value_columns[0]

        window_id = int(data["key"].min() // 10) if "key" in data.columns and not data.empty else 0
        primary_sum = int(data[primary_column].sum())
        secondary_mean = float(data[secondary_column].mean())
        max_key = int(data["key"].max()) if "key" in data.columns and not data.empty else 0

        if mode == MODE_COMPATIBLE_COMBO:
            if window_id == 0:
                return pd.DataFrame(
                    {
                        "metric_double": _series(np.float64(secondary_mean), "float64"),
                        "metric_long": _series(np.int64(primary_sum), "int64"),
                    }
                )
            if window_id == 1:
                return pd.DataFrame(
                    {
                        "metric_double": _series(np.float64(secondary_mean), "float64"),
                        "metric_long": _series(np.int32(primary_sum), "int32"),
                    }
                )
            return pd.DataFrame({"metric_long": _series(np.int32(primary_sum), "int32")})

        if mode == MODE_FLOAT_TO_DOUBLE:
            if window_id == 0:
                return pd.DataFrame({"metric_double": _series(np.float64(secondary_mean), "float64")})
            return pd.DataFrame({"metric_double": _series(np.float32(secondary_mean), "float32")})

        if mode == MODE_NEW_COLUMN:
            if window_id == 0:
                return pd.DataFrame(
                    {
                        "metric_double": _series(np.float64(secondary_mean), "float64"),
                        "metric_long": _series(np.int64(primary_sum), "int64"),
                    }
                )
            return pd.DataFrame(
                {
                    "extra_metric": _series(np.int64(window_id), "int64"),
                    "metric_double": _series(np.float64(secondary_mean), "float64"),
                    "metric_long": _series(np.int64(primary_sum), "int64"),
                }
            )

        if mode == MODE_KEY_MISMATCH:
            if window_id == 0:
                return pd.DataFrame(
                    {
                        "key": _series(np.int64(max_key), "int64"),
                        "metric_long": _series(np.int64(primary_sum), "int64"),
                    }
                )
            return pd.DataFrame({"metric_long": _series(np.int64(primary_sum), "int64")})

        if mode == MODE_TYPE_CONFLICT:
            if window_id == 0:
                return pd.DataFrame({"metric_long": _series(np.int64(primary_sum), "int64")})
            return pd.DataFrame({"metric_long": _series(np.float64(secondary_mean), "float64")})

        raise ValueError(f"Unsupported mode: {mode}")
