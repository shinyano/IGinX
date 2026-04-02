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

import sys

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


def _value_columns(data):
    return [column for column in data.columns if column != "key"]


@UDSFWrapper
class ArchitectureBenchmarkMetadata:
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
class ArchitectureComputeIntensiveBenchmark:
    def eval(self, data, loops=20, *args, **kwargs):
        loops = _normalize_loops(loops)
        columns = _value_columns(data)
        values = data[columns]
        total = 0.0
        for _ in range(loops):
            for row in values.itertuples(index=False, name=None):
                for value in row:
                    fv = float(value)
                    total += fv * fv
        return pd.DataFrame({"result": [total]})
