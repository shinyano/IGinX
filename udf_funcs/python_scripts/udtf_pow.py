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
from iginx_udf import UDTFinDF


class UDFPow(UDTFinDF):
  def eval(self, data, n=1):
    data = data.drop(columns=['key'])
    df_squared = data.applymap(lambda x: float(x ** n))
    df_squared.columns = ["pow({col}, {n})".format(col=col, n=n) for col in df_squared.columns]
    df_squared.astype('float32')
    return df_squared
