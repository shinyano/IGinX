import time
import threading

from iginx_udf import UDSFWrapper

@UDSFWrapper
class TimeoutTest:
    def __init__(self):
        pass

    def timeout(self):
        """dead loop"""
        try:
            limit = 6
            while limit > 0:
                print('running timeout')
                time.sleep(10)
                limit -= 1
        finally:
            pass

    def waitForEvent(self):
        event = threading.Event()
        event.wait()

    def downloadLargeModel(self):
        """download big ML models to test timeout when downloading"""
        print("start to get models...")
        from transformers import BlipProcessor, BlipForConditionalGeneration
        processor = BlipProcessor.from_pretrained("Salesforce/blip-image-captioning-base")
        model = BlipForConditionalGeneration.from_pretrained("Salesforce/blip-image-captioning-base")
        print("finished downloading.")

    def eval(self, data, mode=1, *args, **kwargs):
        if mode == 1:
            self.timeout()
        elif mode == 2:
            self.waitForEvent()
        else:
            self.downloadLargeModel()
