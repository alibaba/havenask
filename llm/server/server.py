#!/usr/bin/env python
# -*- coding: utf-8 -*-

import inspect
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from models.models import (
    Query,
    HAResult,
    QueryResponse,
    QueryStreamResponse
)
from llm_adapter.factory import get_llm_adapter
from havenask.havenask import Havenask
from typing import List
from extractor.file_extractor import extract_file

app = FastAPI()

@app.post("/chat")
async def chat(query: Query):
    embedding = await llm.embed_query(query.query)
    candidate_docs = await Havenask.query(embedding, query.topn)
    result, history = await llm.chat(candidate_docs, query.query, query.history)
    return QueryResponse(success=True, result=result, history=history, source=[doc.source_id for doc in candidate_docs])

@app.post("/stream-chat")
async def stream_chat(query: Query):
    embedding = await llm.embed_query(query.query)
    candidate_docs = await Havenask.query(embedding, query.topn)

    async def streaming(candidate_docs: List[HAResult], query: Query):
        if inspect.iscoroutinefunction(llm.stream_chat):
            response = await llm.stream_chat(candidate_docs, query.query, query.history)
        else:
            response = llm.stream_chat(candidate_docs, query.query, query.history)
        async for result, history in response:
            yield QueryStreamResponse(success=True, result=result, finished=False).json() + '\r\n'
        yield QueryResponse(success=True, result=result, history=history, source=[doc.source_id for doc in candidate_docs]).json() + '\r\n'
    return StreamingResponse(streaming(candidate_docs, query))

@app.on_event("startup")
async def startup():
    global llm
    llm = get_llm_adapter()