#!/usr/bin/env python
# -*- coding: utf-8 -*-

from fastapi import FastAPI, Request
from transformers import AutoTokenizer, AutoModel
import uvicorn, json, datetime
import torch
import time

app = FastAPI()

def mean_pooling(model_output, attention_mask):
    token_embeddings = model_output[0]  # First element of model_output contains all token embeddings
    input_mask_expanded = attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
    return torch.sum(token_embeddings * input_mask_expanded, 1) / torch.clamp(input_mask_expanded.sum(1), min=1e-9)

@app.post("/embed")
async def embed(request: Request):
    global model, tokenizer
    json_post_raw = await request.body()
    print(json_post_raw)
    json_post_list = json.loads(json_post_raw)
    text = json_post_list.get('text')

    begin = time.time()
    encoded_input = tokenizer(text, padding=True, truncation=True, return_tensors='pt', max_length=512)
    with torch.no_grad():
        model_output = model(**encoded_input)
    embeddings = mean_pooling(model_output, encoded_input['attention_mask'])
    end = time.time()
    response = {
        "embedding": embeddings[0].numpy().tolist(),
        "status": 200,
        "time": end - begin
    }
    return response

@app.post("/tokenize")
async def tokenize(request: Request):
    global model, tokenizer
    json_post_raw = await request.body()
    print(json_post_raw)
    json_post_list = json.loads(json_post_raw)
    text = json_post_list.get('text')

    tokens = tokenizer(text, add_special_tokens=False, return_offsets_mapping=True)
    response = {
        'offset_mapping': tokens['offset_mapping']
    }
    return response

if __name__ == '__main__':
    tokenizer = AutoTokenizer.from_pretrained('GanymedeNil/text2vec-large-chinese', use_fast=True)
    model = AutoModel.from_pretrained('GanymedeNil/text2vec-large-chinese')
    if torch.cuda.is_available():
        model.half().cuda()
    else:
        model.float()
    model.eval()
    uvicorn.run(app, host='0.0.0.0', port=8008, workers=1)