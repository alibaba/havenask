import os
import torch
import logging
from transformers import AutoTokenizer, AutoModel
from typing import List
from util.torch_util import torch_gc
from tokenizer.pretrained_tokenizer import PretrainedTokenizer
from .base_embedding import BaseEmbedding

class LocalEmbedding(BaseEmbedding):
    EMBEDDING_MODEL = os.environ.get('EMBEDDING_MODEL') or 'GanymedeNil/text2vec-large-chinese'

    _INSTANCE = None

    def __init__(self):
        self.tokenizer = AutoTokenizer.from_pretrained(self.EMBEDDING_MODEL, use_fast=True)
        self.model = AutoModel.from_pretrained(self.EMBEDDING_MODEL)
        if torch.cuda.is_available():
            self.model.half().cuda()
        else:
            self.model.float()
        logging.info(f'load text embedding model{self.EMBEDDING_MODEL} success')

    def mean_pooling(self, model_output, attention_mask):
        token_embeddings = model_output[0]  # First element of model_output contains all token embeddings
        input_mask_expanded = attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
        return torch.sum(token_embeddings * input_mask_expanded, 1) / torch.clamp(input_mask_expanded.sum(1), min=1e-9)

    async def embed_text(self, text: str) -> List[int]:
        tokens = self.tokenizer(text, padding=True, truncation=True, return_tensors='pt', max_length=512)
        with torch.no_grad():
            model_output = self.model(**tokens)
        embeddings = self.mean_pooling(model_output, tokens['attention_mask'])
        return embeddings[0].numpy().tolist()

    async def embed_query(self, text: str) -> List[int]:
        return await self.embed_text(text)
    
    def get_tokenizer(self) -> PretrainedTokenizer:
        return PretrainedTokenizer(self.tokenizer)

    @classmethod
    def get_instance(cls):
        if cls._INSTANCE is None:
            cls._INSTANCE = LocalEmbedding()
        return cls._INSTANCE