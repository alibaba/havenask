import abc
from typing import List
from models.models import HADocument, HAResult

class BaseStore(abc.ABC):

    @abc.abstractmethod
    async def insert(self, docs: List[HADocument]):
        raise NotImplementedError

    @abc.abstractmethod
    async def query(self, embedding: List[float], topn: int) -> List[HAResult]:
        raise NotImplementedError