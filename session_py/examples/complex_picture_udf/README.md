# 数据处理流程设计：

- 原有数据：一个文件系统引擎（图片文件夹），一个保存图片元数据信息的关系表
  - 图片文件夹：ori
    - camera.jpg
    - horse.jpg
    - notebook.jpg
    - pizza.jpg
    - wedding.jpg
  - 元数据关系表：pics.image_descriptions

pics.image_descriptions.name
pics.image_descriptions.description
类型
BINARY
BINARY
描述
图片文件名
图片旧描述
pics.image_descriptions.name
pics.image_descriptions.description
'camera\\jpg'
'camera'
'horse\\jpg'
'horse'
'notebook\\jpg'
'notebook'
'pizza\\jpg'
'pizza'
'wedding\\jpg'
'wedding'
- UDF使用AI对每张图片生成新的描述，与旧描述拼接并加到图片上，返回新图片的二进制数据，每张图片会作为列返回，便于使用outfile将每张图片作为单独文件导出。在需要存入IGinX的时候也可以进行简单修改为按行输出
- UDF返回数据：

key

udf_img_cap(name1)
udf_img_cap(name2)
...
类型
long
BINARY
...
...
描述
图片id，此处也作为key
新图片的二进制数据
...
...
- 使用outfile功能导出新图片数据文件到本地文件或者存入IGinX

详细实现步骤：
Step 1：准备原始数据
准备五张原始图片，存在ori文件夹中：
图片链接：
camera, horse, notebook, pizza, wedding
插入元信息到关系型数据库，这里使用mysql命令行

```sql
CREATE DATABASE pics;
USE pics;

DROP TABLE image_descriptions;

CREATE TABLE image_descriptions (
name varchar(25) PRIMARY KEY,
description TEXT
);

INSERT INTO image_descriptions (name, description) VALUES
('camera\\jpg', 'camera'),
('horse\\jpg', 'horse'),
('notebook\\jpg', 'notebook'),
('pizza\\jpg', 'pizza'),
('wedding\\jpg', 'wedding');
```

Step 2：注册UDF
需要用到两个UDF，一个是为没有key的表格添加key，一个是图片处理UDF
脚本文件：
- udf_img_cap:
暂时无法在飞书文档外展示此内容
- add_key:
暂时无法在飞书文档外展示此内容

SQL:
create function udsf "udf_img_cap" from "ImgUDF" in "pics_example.py";
-- 该UDF接收参数：
--     第一列为key，后面每列为一张图片的二进制数据
--     每列最后一行是旧caption，前面的行拼起来是完整的图片数据（因为查询时会按每行1m数据的格式进行分割）

create function udsf "add_key" from "UDFAddKey" in "udf_add_key.py";
-- 该UDF是为没有key的表添加从0开始的key，这个UDF主要是为了拼SQL查询
Step 3：使用udf处理数据并使用outfile将新图片load到本地：
-- UDF中需要import torch等大型库，先用这个语句初始化一下UDF脚本，否则直接执行下面的复杂语句时可能会在初始化UDF时卡死
explain select udf_img_cap(*) from pics;

-- 一次性处理所有图片
select udf_img_cap(*) from (
select * from (
select value2meta(select name from pics.image_descriptions)
from ori)
-- 获得所有的图片数据
union
select add_key(*)
from (
select transpose(*) from (
select description from pics.image_descriptions))
-- 选出所有图片的旧caption，转置变成列后用union拼到图片数据的最后一行
) into outfile "out" as stream;
[图片]
outfile结果：
[图片]
需要将后缀名改为jpg：
[图片]
生成的图片示例：
[图片]

讨论
select UDF(description) from pics.image_descriptions
（查询树有什么特点？）
==>
select UDF(*) from (select description from pics.image_descriptions)
（转换成这样的查询树？）

