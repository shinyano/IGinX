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

import math
import pandas as pd
from iginx_udf import UDTFWrapper

@UDTFWrapper
class UDFArcCos:
    def __init__(self):
        pass

    def eval(self, data, *args, **kwargs):
        cols = [c for c in data.columns if c != "key"]
        result = {}
        for col in cols:
            try:
                result["arccos(" + col + ")"] = [math.acos(data[col].iloc[0])]
            except ValueError:
                result["arccos(" + col + ")"] = [None]
        return pd.DataFrame(result)
