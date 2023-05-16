import os
import logging
import json
import aiohttp
import urllib.parse
from typing import List
from tenacity import retry, wait_random_exponential, stop_after_attempt
from tokenizer.remote_tokenizer import RemoteTokenizer
from .base_embedding import BaseEmbedding

class RemoteEmbedding(BaseEmbedding):
    EMBEDDING_ENDPOINT = os.environ.get('EMBEDDING_SERVER_ENDPOINT')

    _INSTANCE = None

    @retry(wait=wait_random_exponential(min=1, max=20), stop=stop_after_attempt(3))
    async def embed_text(self, text: str) -> List[float]:
        data = { 'text': text }
        headers = {'content-type': 'application/json'}
        api = urllib.parse.urljoin(self.EMBEDDING_ENDPOINT, 'embed')
        async with aiohttp.ClientSession() as session:
            async with session.post(api, data=json.dumps(data), headers=headers) as response:
                text = await response.text()
                logging.debug(text)
                text_json = json.loads(text)
                return text_json['embedding']

    @retry(wait=wait_random_exponential(min=1, max=20), stop=stop_after_attempt(3))
    async def embed_query(self, text: str) -> List[float]:
        return await self.embed_text(text)

    def get_tokenizer(self):
        api = urllib.parse.urljoin(self.EMBEDDING_ENDPOINT, 'tokenize')
        return RemoteTokenizer(api)

    @classmethod
    def get_instance(cls):
        if cls._INSTANCE is None:
            cls._INSTANCE = RemoteEmbedding()
        return cls._INSTANCE