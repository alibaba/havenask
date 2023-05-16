#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing import List, Optional, Any
from pydantic import BaseModel

class Document(BaseModel):
    id: str
    content: str
    url: Optional[str] = None

class DocumentList(BaseModel):
    documents: List[Document]

class Query(BaseModel):
    query: str
    history: Optional[List[Any]] = []
    topn: Optional[int] = 3

class HAResult(BaseModel):
    pk: str
    content: str
    source_id: str

class HADocument(BaseModel):
    id: str
    source_id: str
    content: str
    url: Optional[str] = None
    embedding: List[float]

    @property
    def pk(self):
        return self.source_id + '_' + self.id

    def to_ha_str(self) -> str:
        doc_str = 'CMD=add\x1F\n'
        doc_str += f'pk={self.pk}\x1F\n'
        doc_str += f'source_id={self.source_id}\x1F\n'
        doc_str += f'content={self.content}\x1F\n'
        if self.url is not None:
            doc_str += f'url={self.url}\x1F\n'
        doc_str += f'embedding={",".join([str(p) for p in self.embedding])}\x1F\n'
        doc_str += '\x1E\n'
        return doc_str

class InsertResponse(BaseModel):
    success: bool

class QueryResponse(BaseModel):
    success: bool
    result: str
    history: List[Any]
    source: List[str]
    finished: Optional[bool] = True

class QueryStreamResponse(BaseModel):
    success: bool
    result: str
    finished: bool

class  ChunkConfig(BaseModel):
    max_count: int
    size: int
    overlap: int