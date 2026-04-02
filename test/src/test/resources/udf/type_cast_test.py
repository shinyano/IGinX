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
from iginx_udf import UDSFWrapper

@UDSFWrapper
class TypeCastTest():
    def eval(self, data, *args, **kwargs):
        rows = [
            [1, 23372, 567, 1, 9999],
            [0.5, 2.71828, 9.876, 2.5, 3.1415926535],
            [True, False, True, False, True],
            ["b", "-453625", "5.327", "false", "aaa"],
        ]
        col_names = ["row0", "row1", "row2", "row3"]
        return pd.DataFrame(rows).T.set_axis(col_names, axis=1)
