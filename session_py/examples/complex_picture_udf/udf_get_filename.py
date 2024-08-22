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

class UDFGetFileName:
    """
    从文件系统数据库的数据路径中解析出文件名（后缀中的\\不进行转换）
    例：文件ori中image.jpg
    查询出ori.image\jpg， 该UDF解析出image\jpg
    仅接收一列数据
    """
    def __init__(self):
        pass
    def transform(self, data, args, kvargs):
        res = self.buildHeader(data)
        data = data[2:]
        for index, sublist in enumerate(data):
            sublist[1] = sublist[1].split(b'.')[-1]
            # sublist[0] = index
            del sublist[0]
        res.extend(data)
        return res
    def buildHeader(self, data):
        ret_name = []
        for name in data[0][1:]:
            ret_name.append(f"get_filename({name})")
        # always has key, but values are -1
        return [ret_name, ["BINARY"]]
        # return [data[0], data[1]]
