import os
import torch
import logging
from transformers import AutoTokenizer, AutoModel
from util.torch_util import torch_gc

class LocalChatGLM(object):
    MODEL_NAME = os.environ.get('CHATGLM_MODEL') or 'THUDM/chatglm-6b'
    _INSTANCE = None

    def __init__(self):
        self.tokenizer = AutoTokenizer.from_pretrained(self.MODEL_NAME, trust_remote_code=True)
        self.model = AutoModel.from_pretrained(self.MODEL_NAME, trust_remote_code=True)
        if torch.cuda.is_available():
            self.model.half().cuda()
        else:
            self.model.float()
        self.model.eval()
        logging.info(f'load chatglm model[{self.MODEL_NAME}] success')

    async def chat(self, prompt, history, max_length, top_p, temperature):
        response, history = self.model.chat(self.tokenizer,
                                            prompt,
                                            history=history,
                                            max_length=max_length,
                                            top_p=top_p,
                                            temperature=temperature)
        torch_gc()
        logging.info(f'prompt:{prompt}, response:{repr(response)}')
        return response, history

    async def stream_chat(self, prompt, history, max_length, top_p, temperature):
        for response, history in self.model.stream_chat(self.tokenizer,
                                                        prompt,
                                                        history=history,
                                                        max_length=max_length,
                                                        top_p=top_p,
                                                        temperature=temperature):
            yield response, history
        logging.info(f'prompt:{prompt}, response:{repr(response)}')
        torch_gc()

    @classmethod
    def get_instance(cls):
        if cls._INSTANCE is None:
            cls._INSTANCE = LocalChatGLM()
        return cls._INSTANCE
