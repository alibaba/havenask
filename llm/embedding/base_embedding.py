import abc
from typing import List

class BaseEmbedding(abc.ABC):

    @abc.abstractmethod
    async def embed_text(self, text: str) -> List[float]:
        raise NotImplementedError

    @abc.abstractmethod
    async def embed_query(self, text: str) -> List[float]:
        raise NotImplementedError

    @abc.abstractmethod
    def get_tokenizer(self):
        raise NotImplementedError