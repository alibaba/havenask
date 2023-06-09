#!/usr/bin/env python
# -*- coding: utf-8 -*-

import inspect
from fastapi import FastAPI, UploadFile
from fastapi.responses import StreamingResponse
from models.models import (
    Query,
    HAResult,
    QueryResponse,
    QueryStreamResponse,
    HADocument,
    DocumentList,
    InsertResponse
)
from llm_adapter.factory import get_llm_adapter
from vectorstore.factory import get_vector_store
from typing import List
from spliter.sentence_spliter import SentenceSpliter
from extractor.file_extractor import extract_file

app = FastAPI()

@app.post("/files")
async def upload_file(file: UploadFile):
    file_text = await extract_file(file)
    spliter = SentenceSpliter()
    chunks = await spliter.split_token_with_overlap(file_text)
    if len(chunks) == 0:
        return InsertResponse(success=True)

    embeddings = []
    for chunk in chunks:
        embedding = await llm.embed_text(chunk)
        embeddings.append(embedding)
    ha_documents = []
    for i, chunk in enumerate(chunks):
        ha_document = HADocument(id=i, source_id=file.filename, content=chunk, embedding=embeddings[i])
        ha_documents.append(ha_document)
    await store.insert(ha_documents)
    return InsertResponse(success=True)

@app.post("/documents")
async def insert(request: DocumentList):
    spliter = SentenceSpliter()
    for doc in request.documents:
        chunks = await spliter.split_token_with_overlap(doc.content)
        embeddings = []
        for chunk in chunks:
            embedding = await llm.embed_text(chunk)
            embeddings.append(embedding)
        ha_documents = []

        for i, chunk in enumerate(chunks):
            ha_document = HADocument(id=i, source_id=doc.id, content=chunk, embedding=embeddings[i])
            ha_documents.append(ha_document)
        await store.insert(ha_documents)
    return InsertResponse(success=True)

@app.post("/chat")
async def chat(query: Query):
    embedding = await llm.embed_query(query.query)
    candidate_docs = await store.query(embedding, query.topn)
    result, history = await llm.chat(candidate_docs, query.query, query.history)
    return QueryResponse(success=True, result=result, history=history, source=[doc.source_id for doc in candidate_docs])

@app.post("/stream-chat")
async def stream_chat(query: Query):
    embedding = await llm.embed_query(query.query)
    candidate_docs = await store.query(embedding, query.topn)

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
    global llm, store
    llm = get_llm_adapter()
    store = get_vector_store()