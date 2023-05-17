import tiktoken
from .tokenizer import Tokenizer
from typing import List

class OpenAITokenizer(Tokenizer):
    def __init__(self, encoding_name: str):
        self._tokenizer = tiktoken.get_encoding(encoding_name)

    async def encode(self, text: str) -> List[int]:
        return self._tokenizer.encode(text)
    
    def decode(self, raw_text: str, tokens: List[int]) -> str:
        return self._tokenizer.decode(tokens)