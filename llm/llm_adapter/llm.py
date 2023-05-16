from abc import ABC, abstractclassmethod
from typing import List, Any
from models.models import HAResult, ChunkConfig

class LLM(ABC):
    
    @abstractclassmethod
    async def embed_text(cls, text: str) -> List[float]:
        raise NotImplementedError

    @abstractclassmethod
    async def embed_query(cls, text: str) -> List[float]:
        raise NotImplementedError

    @abstractclassmethod
    async def chat(cls, contexts: List[HAResult], query: str, history: List[Any]):
        raise NotImplementedError

    @classmethod
    async def stream_chat(cls, contexts: List[HAResult], query: str, history: List[Any]):
        raise NotImplementedError

    @abstractclassmethod
    def get_tokenizer(cls):
        raise NotImplementedError

    @classmethod
    def get_chunk_config(cls) -> ChunkConfig:
        raise NotImplementedError

