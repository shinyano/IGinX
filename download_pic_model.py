from transformers import BlipProcessor, BlipForConditionalGeneration

processor = BlipProcessor.from_pretrained("Salesforce/blip-image-captioning-base")
model = BlipForConditionalGeneration.from_pretrained("Salesforce/blip-image-captioning-base")

processor.save_pretrained("local_model/blip_processor")
model.save_pretrained("local_model/blip_model")

print("下载完成")