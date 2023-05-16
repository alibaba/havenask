#!/usr/bin/env python
# -*- coding: utf-8 -*-

from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from transformers import AutoTokenizer, AutoModel
import uvicorn, json, datetime
import torch
import time
from pydantic import BaseModel
from typing import List, Any, Optional

DEVICE = "cuda"
DEVICE_ID = "0"
CUDA_DEVICE = f"{DEVICE}:{DEVICE_ID}" if DEVICE_ID else DEVICE


def torch_gc():
    if torch.cuda.is_available():
        with torch.cuda.device(CUDA_DEVICE):
            torch.cuda.empty_cache()
            torch.cuda.ipc_collect()


app = FastAPI()

class ChatRequest(BaseModel):
    prompt: str
    history: List[Any]
    max_length: Optional[int] = 2048
    top_p: Optional[float] = 0.7
    temperature: Optional[float] = 0.01

@app.post("/chat")
async def chat(request: ChatRequest):
    global model, tokenizer
    begin = time.time()
    response, history = model.chat(tokenizer,
                                   request.prompt,
                                   history=request.history,
                                   max_length=request.max_length,
                                   top_p=request.top_p,
                                   temperature=request.temperature)
    now = datetime.datetime.now()
    now_time = now.strftime("%Y-%m-%d %H:%M:%S")
    answer = {
        "response": response,
        "history": history,
        "latency": time.time() - begin
    }
    log = "[" + now_time + "] " + '", prompt:"' + request.prompt + '", response:"' + repr(response) + '"'
    print(log)
    torch_gc()
    return answer

@app.post("/stream-chat")
async def stream_chat(request: ChatRequest):
    global model, tokenizer

    print(request.dict())

    def streaming(request):
        begin = time.time()
        response = ''
        history = []
        for response, history in model.stream_chat(tokenizer,
                                                   request.prompt,
                                                   history=request.history,
                                                   max_length=request.max_length,
                                                   top_p=request.top_p,
                                                   temperature=request.temperature):
            answer = {
                "response": response,
                "finished": False,
                "status": 200
            }
            yield json.dumps(answer) + '\r\n'
        final_result =  {
            "response": response,
            "history": history,
            "finished": True,
            "status": 200,
            "latency": time.time() - begin
        }
        now = datetime.datetime.now()
        now_time = now.strftime("%Y-%m-%d %H:%M:%S")
        log = "[" + now_time + "] " + '", prompt:"' + request.prompt + '", response:"' + repr(response) + '"'
        print(log)
        torch_gc()
        yield json.dumps(final_result) + '\r\n'

    return StreamingResponse(streaming(request))

if __name__ == '__main__':
    tokenizer = AutoTokenizer.from_pretrained("THUDM/chatglm-6b", trust_remote_code=True)
    model = AutoModel.from_pretrained("THUDM/chatglm-6b",trust_remote_code=True)
    if torch.cuda.is_available():
        model.half().cuda()
    else:
        model.float()
    model.eval()
    uvicorn.run(app, host='0.0.0.0', port=8001, workers=1)