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
from iginx_udf import UDSFWrapper
from PIL import Image
import io

# 测试arrow传输图像二进制数据

@UDSFWrapper
class UDFImgTest:
    def __init__(self):
        pass

    def eval(self, data, *args):
        print(data)
        # 假设 df 的 image 列里存的是 bytes
        img_bytes = data.loc[0, 'dir.tiny\\.png']

        # 从二进制读取图片
        img = Image.open(io.BytesIO(img_bytes))

        # 打开显示
        img.show()
        resultdf = pd.DataFrame({'col': [1]})
        return resultdf

# select * from dir;
# create function udsf "udf_img" from "UDFImgTest" in "E:\\IGinX_Lab\\local\\IGinX\\udsf_testimg.py";
# select udf_img(*, 1) from dir;
# drop function "udf_img";
