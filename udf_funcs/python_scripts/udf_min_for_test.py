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

import time
class UDFMin:
    def __init__(self):
        pass

    def transform(self, data, args, kvargs):
        return int(time.time() * 1000)
        # start = time.perf_counter()
        # res = self.buildHeader(data)
        #
        # minRow = []
        # rows = data[2:]
        # for row in list(zip(*rows))[1:]:
        #     min = None
        #     for num in row:
        #         if num is not None:
        #             if min is None:
        #                 min = num
        #             elif min > num:
        #                 min = num
        #     minRow.append(min)
        # res.append(minRow)
        # end = time.perf_counter()
        # print(f"[PYTHON] compute took{(end - start)*1000}ms")
        # return res

    def buildHeader(self, data):
        colNames = []
        for name in data[0][1:]:
            colNames.append("udf_min(" + name + ")")
        return [colNames, data[1][1:]]
