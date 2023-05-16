import os
import aiohttp
import logging
import json
import urllib.parse
from typing import List
from tenacity import retry, wait_random_exponential, stop_after_attempt

class RemoteChatGLM(object):
    CHATGLM_ENDPOINT = os.environ.get('CHATGLM_SERVER_ENDPOINT')

    _INSTANCE = None

    @retry(wait=wait_random_exponential(min=1, max=20), stop=stop_after_attempt(3))
    async def chat(cls, prompt: str, history: List[List[str]], max_length, top_p, temperature):
        data = {
            'prompt': prompt,
            'history': history,
            'max_length': max_length,
            'top_p': top_p,
            'temperature': temperature
        }
        logging.debug(data)
        headers = {'content-type': 'application/json'}
        async with aiohttp.ClientSession() as session:
            api = urllib.parse.urljoin(cls.CHATGLM_ENDPOINT, '/chat')
            async with session.post(api, data=json.dumps(data), headers=headers) as response:
                text = await response.text()
                json_text = json.loads(text)
                return json_text['response'], json_text['history']

    @retry(wait=wait_random_exponential(min=1, max=20), stop=stop_after_attempt(3))
    async def stream_chat(cls, prompt: str, history: List[List[str]], max_length, top_p, temperature):
        data = {
            'prompt': prompt,
            'history': history,
            'max_length': max_length,
            'top_p': top_p,
            'temperature': temperature
        }
        logging.debug(data)
        headers = {'content-type': 'application/json'}
        async with aiohttp.ClientSession() as session:
            api = urllib.parse.urljoin(cls.CHATGLM_ENDPOINT, '/stream-chat')
            async with session.post(api, data=json.dumps(data), headers=headers) as response:
                async for text in response.content:
                    json_text = json.loads(text)
                    yield json_text['response'], json_text.get('history', [])

    @classmethod
    def get_instance(cls):
        if cls._INSTANCE is None:
            cls._INSTANCE = RemoteChatGLM()
        return cls._INSTANCE