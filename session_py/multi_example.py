import cv2
import numpy as np
import torch
from transformers import BlipProcessor, BlipForConditionalGeneration, VisionEncoderDecoderModel, ViTFeatureExtractor, AutoTokenizer
from PIL import Image, ImageDraw, ImageFont
import io
import base64

class ImgUDF:
    def __init__(self):
        self.processor = BlipProcessor.from_pretrained("Salesforce/blip-image-captioning-base")
        self.model = BlipForConditionalGeneration.from_pretrained("Salesforce/blip-image-captioning-base")
        # # 加载特征提取器、模型和分词器
        # self.feature_extractor = ViTFeatureExtractor.from_pretrained("nlpconnect/vit-gpt2-image-captioning")
        # self.tokenizer = AutoTokenizer.from_pretrained("nlpconnect/vit-gpt2-image-captioning")
        # self.model = VisionEncoderDecoderModel.from_pretrained("nlpconnect/vit-gpt2-image-captioning")
        #
        # # 如果有GPU可用，使用GPU加速
        # self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        # self.model.to(self.device)

    def generate_caption(self, image_data):
        print("starting to generate caption...")
        image = Image.open(io.BytesIO(image_data))
        print("1")
        inputs = self.processor(images=image, return_tensors="pt")
        print("2")
        out = self.model.generate(**inputs)
        print("3")
        caption = self.processor.decode(out[0], skip_special_tokens=True)
        print(f"caption:{caption}")

        return caption
        # print("Starting to generate caption...")
        # try:
        #     # 将字节数据转换为图像对象
        #     image = Image.open(io.BytesIO(image_data))
        #     print("Image loaded successfully.")
        #
        #     # 使用特征提取器预处理图像
        #     pixel_values = self.feature_extractor(images=image, return_tensors="pt").pixel_values
        #     pixel_values = pixel_values.to(self.device)
        #     print("Image preprocessed successfully.")
        #
        #     # 生成图像描述
        #     output_ids = self.model.generate(pixel_values)
        #     print("Caption generated.")
        #
        #     # 解码生成的描述
        #     caption = self.tokenizer.decode(output_ids[0], skip_special_tokens=True)
        #     print(f"Caption: {caption}")
        #
        #     return caption
        # except Exception as e:
        #     print(f"An error occurred: {e}")
        #     return "Error generating caption."

    def create_annotated_image(self, image_data, description, save_path=""):
        print("starting to create new image...")
        image = Image.open(io.BytesIO(image_data))
        draw = ImageDraw.Draw(image)
        font = ImageFont.load_default()
        text_position = (10, 10)
        draw.text(text_position, description, font=font, fill="white")
        output = io.BytesIO()
        image.save(output, format='JPEG')
        if save_path != "":
            image.save(save_path, format='JPEG')
        print("created.")

        return output.getvalue()

    def process_image(self, image_data, description):
        # image_data = base64.b64decode(image_data.decode('utf-8'))
        new_description = self.generate_caption(image_data)

        # 合并原始描述和新的描述
        full_description = description + " | " + new_description
        new_image_data = self.create_annotated_image(image_data, full_description)

        return new_image_data

    def transform(self, data, args, kvargs):
        res = self.buildHeader(data)
        for row in data[2:]:
            print(type(row[2]))
            print(row[3])
            new_img = self.process_image(row[2], row[3])
            res.append([row[1], new_img])
        return res

    def buildHeader(self, data):
        retName = "udf_img_cap("
        for name in data[0][1:]:
            retName += name + ", "
        retName = retName[:-2] + ")"
        return [["key", retName], ["LONG", "BINARY"]]
