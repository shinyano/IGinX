# 分支使用说明：

对应文档：[‌﻿‌‍‌‌⁠‬⁠‍‬‌‌‌‍‬﻿﻿‌‍python 新旧方法测试 v2.0 - Feishu Docs](https://oxlh5mrwi0.feishu.cn/docx/ZGeSdCRnSo4grXxWCE5cG1lKn4h)

测试方法：

- RequestContext 调 batchRowCount（每个batch多少行）
- FilterExecutor调InvokeType INVOKE_TYPE 切换新旧模式
  - 旧模式：二维列表交互
  - 新模式：arrow通过c data transfer 交互
    - 注意每个进程中有一次JNILoadder初始化时间，因此需要跳过前几次执行。
    - arrow最好跳过前5~7次，之后数据会趋于稳定
- client执行explain physical sql查询，filter算子的孩子节点的row数就是会被测试的数据总行数，batch根据batchRowCount进行分割。
- 每次结果会展示各个batch执行时间、行数、总时间、batch平均执行时间