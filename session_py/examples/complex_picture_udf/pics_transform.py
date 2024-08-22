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

import numpy as np
import torch
from transformers import BlipProcessor, BlipForConditionalGeneration
from PIL import Image, ImageDraw, ImageFont

class ImgUDF:
    """
    This UDF class takes table:{ image_id:long | image_data:bytes | old_caption:bytes }
    and generate new caption based on the image_data using model:"Salesforce/blip-image-captioning-base",
    then put "<old_caption> | <new_caption>"text onto the image as new image
    finally returns bytes of new images as table:{ key:long(same as image_id) | new_image_data:bytes }
    """
    def __init__(self):
        import os
        # self.processor = BlipProcessor.from_pretrained("Salesforce/blip-image-captioning-base")
        # self.model = BlipForConditionalGeneration.from_pretrained("Salesforce/blip-image-captioning-base")
        processor_path = os.path.abspath("local_model/blip_processor")
        model_path = os.path.abspath("local_model/blip_model")
        print(f"Processor saved at: {processor_path}")
        print(f"Model saved at: {model_path}")
        self.processor = BlipProcessor.from_pretrained("local_model/blip_processor")
        self.model = BlipForConditionalGeneration.from_pretrained("local_model/blip_model")

    def generate_caption(self, image_data):
        import io
        image = Image.open(io.BytesIO(image_data))
        inputs = self.processor(images=image)
        pixel_values = np.array(inputs["pixel_values"][0])
        pixel_values = torch.from_numpy(pixel_values).unsqueeze(0)
        pixel_values = pixel_values.to(self.model.device)
        out = self.model.generate(pixel_values)
        caption = self.processor.decode(out[0], skip_special_tokens=True)
        return caption

    def create_annotated_image(self, image_data, description, save_path=""):
        import io
        import textwrap
        image = Image.open(io.BytesIO(image_data))
        draw = ImageDraw.Draw(image)
        font = ImageFont.load_default(200)
        max_width = image.width - 20
        wrapped_text = textwrap.fill(description, width=max_width // 200)
        text_position = (10, 10)
        draw.text(text_position, wrapped_text, font=font, fill="white")
        output = io.BytesIO()
        image.save(output, format='JPEG')
        if save_path != "":
            image.save(save_path, format='JPEG')
        return output.getvalue()

    def process_image(self, image_data, description):
        new_description = self.generate_caption(image_data)

        # 合并原始描述和新的描述
        full_description = description.decode('utf-8') + " | " + new_description
        new_image_data = self.create_annotated_image(image_data, full_description)

        return new_image_data

    def transform(self, data):
        res = self.buildHeader(data)
        res_col = [0]
        # data每行均为bytes类型
        np_data = np.array(data[1:], dtype=object)
        print(np_data.shape)
        for col in range(1, np_data.shape[1]):
            image = bytearray()
            for row in range(np_data.shape[0] - 1):
                element = np_data[row, col]
                print(type(element))
                if element is None:
                    continue
                elif isinstance(element, bytes):
                    image += element
                else:
                    try:
                        image += bytes(element)
                    except TypeError:
                        print(f"Error converting element at row {row}, col {col}: {element}")
                        continue
            new_img = self.process_image(image, np_data[np_data.shape[0] - 1, col])
            print(type(new_img))
            print(len(new_img))
            res_col.append(new_img)
        res.append(res_col)
        return res

    def buildHeader(self, data):
        ret_name = ["key"]
        for name in data[0][1:]:
            ret_name.append(f"udf_img_cap({name})")
        return [ret_name]
