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

class UDFRemoveKey:
    """
    为没有key的结果添加从0开始递增的key
    """
    def __init__(self):
        pass

    def transform(self, data, args, kvargs):
        res = self.buildHeader(data)
        data = data[2:]
        for index, sublist in enumerate(data):
            del sublist[0]
        res.extend(data)
        return res

    def buildHeader(self, data):
        names = [f"remove_key({name})" for name in data[0][1:]]
        type = data[1][1:]
        # always has key, but values are -1
        return [names, type]
