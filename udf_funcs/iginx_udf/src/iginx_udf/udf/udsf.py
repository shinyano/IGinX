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

import pandas as pd

from .udf_base import UDFWrapper


class UDSFWrapper(UDFWrapper):
    def get_udf_type(self) -> str:
        return "udsf"

    def transform(self, data, *args, **kwargs):
        df, original_types, has_key = self._list_to_dataframe(data)
        user_args, user_kwargs = self._unpack_java_params(args)
        result = self._wrapped.eval(df, *user_args, **user_kwargs)
        if isinstance(result, pd.DataFrame):
            return self._dataframe_to_list(result, original_types, has_key)
        return result
