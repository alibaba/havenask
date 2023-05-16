from typing import List, Tuple
from .tokenizer import Tokenizer

class PretrainedTokenizer(Tokenizer):
    def __init__(self, pretrained_tokenizer):
        self._tokenizer = pretrained_tokenizer 

    async def encode(self, text: str) -> List[Tuple[int, int]]:
        tokens = self._tokenizer(text, add_special_tokens=False, return_offsets_mapping=True)
        return tokens['offset_mapping']
    
    def decode(self, raw_text: str, tokens: List[Tuple[int, int]]) -> str:
        if len(tokens) == 0:
            return ''
        start_pos = tokens[0][0]
        end_pos = tokens[-1][1]
        return raw_text[start_pos:end_pos]
