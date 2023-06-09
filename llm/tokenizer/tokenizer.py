from abc import ABC, abstractmethod
from typing import List, Any

class Tokenizer(ABC):

    @abstractmethod
    async def encode(self, text: str) -> List[Any]:
        raise NotImplementedError

    @abstractmethod
    def decode(self, raw_text: str, tokens: List[Any]) -> str:
        raise NotImplementedError