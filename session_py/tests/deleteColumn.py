# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# 无法单独执行，用来测试PySessionIT
import sys
sys.path.append('../session_py/')  # 将上一级目录添加到Python模块搜索路径中

from iginx.iginx_pyclient.session import Session


if __name__ == '__main__':
    session = Session('127.0.0.1', 6888, "root", "root")
    session.open()
    try:
        # 删除部分数据
        session.delete_time_series("a.b.b")
    except Exception as e:
        if str(e) == ("Error occurs: Unable to delete data from read-only nodes. The data of the writable nodes has "
                      "been cleared."):
            exit(0)
        print(e)
        exit(1)
    finally:
        # 查询删除后剩余的数据
        dataset = session.query(["a.*"], 0, 10)
        print(dataset)
        session.close()