import aiohttp
import logging
import json
from typing import List, Tuple
from .tokenizer import Tokenizer
from tenacity import retry, wait_random_exponential, stop_after_attempt

class RemoteTokenizer(Tokenizer):
    def __init__(self, api):
        self._api = api

    @retry(wait=wait_random_exponential(min=1, max=20), stop=stop_after_attempt(3))
    async def encode(self, text: str) -> List[Tuple[int, int]]:
        headers = {'content-type': 'application/json'}
        data = {'text': text}
        async with aiohttp.ClientSession() as session:
            async with session.post(self._api, data=json.dumps(data), headers=headers) as response:
                text = await response.text()
                logging.debug(text)
                text_json = json.loads(text)
                return text_json['offset_mapping']

    def decode(self, raw_text: str, tokens: List[Tuple[int, int]]) -> str:
        if len(tokens) == 0:
            return ''
        start_pos = tokens[0][0]
        end_pos = tokens[-1][1]
        return raw_text[start_pos:end_pos]
