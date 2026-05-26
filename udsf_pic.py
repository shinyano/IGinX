import numpy as np
import torch
from transformers import BlipProcessor, BlipForConditionalGeneration
from PIL import Image, ImageDraw, ImageFont
import pandas as pd
import io
import textwrap
from iginx_udf import UDSFWrapper

processor = BlipProcessor.from_pretrained("E:/IGinX_Lab/local/IGinX/local_model/blip_processor")
model = BlipForConditionalGeneration.from_pretrained("E:/IGinX_Lab/local/IGinX/local_model/blip_model")

@UDSFWrapper
class ImgUDF:
    """
    接收包含图像二进制数据的 DataFrame，每列对应一张图片。
    使用 BLIP 模型为每张图片生成 caption，将 caption 写在图片左上角，
    返回包含新图像二进制数据的 DataFrame。
    """

    def __init__(self):
        self.processor = processor
        self.model = model

    def generate_caption(self, image_data: bytes) -> str:
        image = Image.open(io.BytesIO(image_data)).convert("RGB")
        inputs = self.processor(images=image, return_tensors="pt")
        pixel_values = inputs["pixel_values"].to(self.model.device)
        out = self.model.generate(pixel_values)
        caption = self.processor.decode(out[0], skip_special_tokens=True)
        return caption

    def annotate_image(self, image_data: bytes, caption: str) -> bytes:
        image = Image.open(io.BytesIO(image_data)).convert("RGB")

        # 拍立得风格参数
        border_side = 40        # 左右白边
        border_top = 40         # 上白边
        border_bottom = 400     # 下方白色区域（放文字）

        # 新画布：原图 + 四周白边
        new_width = image.width + border_side * 2
        new_height = image.height + border_top + border_bottom
        canvas = Image.new("RGB", (new_width, new_height), "white")

        # 将原图贴到画布上
        canvas.paste(image, (border_side, border_top))

        # 在下方白色区域写 caption
        draw = ImageDraw.Draw(canvas)
        font = ImageFont.load_default(size=100)

        # 自动换行
        max_chars = max(1, (new_width - border_side * 2) // 36)
        wrapped = textwrap.fill(caption, width=max_chars)

        # 计算文字居中位置
        bbox = draw.textbbox((0, 0), wrapped, font=font)
        text_width = bbox[2] - bbox[0]
        text_height = bbox[3] - bbox[1]
        text_x = (new_width - text_width) // 2
        text_y = image.height + border_top + (border_bottom - text_height) // 2

        draw.text((text_x, text_y), wrapped, font=font, fill="black")

        output = io.BytesIO()
        canvas.save(output, format="JPEG")
        return output.getvalue()

    def eval(self, data: pd.DataFrame, *args) -> pd.DataFrame:
        result = {}
        for col in data.columns:
            if col == 'key':
                continue
            image = bytearray()
            for val in data[col]:
                if val is None:
                    continue
                if isinstance(val, (bytes, bytearray)):
                    image += val
                else:
                    try:
                        image += bytes(val)
                    except TypeError:
                        continue
            if len(image) == 0:
                continue
            caption = self.generate_caption(bytes(image))
            new_img = self.annotate_image(bytes(image), caption)
            result[f"udf_img_{col}"] = [new_img]
        return pd.DataFrame(result)

"""
show columns;

select * from pics.image_locations;

SHOW COLUMNS ori.*;

create function udsf "udf_img" from "ImgUDF" in "E:\\IGinX_Lab\\local\\IGinX\\udsf_pic.py";

SELECT udf_img(*, 1) FROM (
    SELECT value2meta(
        SELECT name FROM pics.image_locations
        WHERE location = 'Paris'
    ) FROM ori
) INTO OUTFILE "E:\\Uni\\grad_proj\\demo\\out" AS STREAM;

SELECT udf_img(*, 1) FROM (
    SELECT value2meta(
        SELECT name FROM pics.image_locations
        WHERE author = 'Marco Rossi'
    ) FROM ori
) INTO OUTFILE "E:\\Uni\\grad_proj\\demo\\out" AS STREAM;

drop function "udf_img";
"""
