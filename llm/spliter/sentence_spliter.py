#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing import List
from llm_adapter.factory import get_llm_adapter

class SentenceSpliter(object):
    def __init__(self):
        self._tokenizer = get_llm_adapter().get_tokenizer()
        self._chunk_config = get_llm_adapter().get_chunk_config()

    async def split(self, content: str) -> List[str]:
        if len(content) == 0:
            return []
        chunks = await self.split_text(content)
        chunks = self.post_process(chunks)
        return chunks

    async def split_text(self, content: str) -> List[str]:
        chunks = []
        splits = content.split('\n')
        for split in splits:
            token_count = len(self._tokenizer.encode(split))
            if token_count < self._chunk_config.size:
                chunks.append(split)
            else:
                new_splits = self.split_text_with_seperator(['?', '!', '.', '？', '！', '。'], split)
                for new_split in new_splits:
                    if len(self._tokenizer.encode(new_split)) < self._chunk_config.size:
                        chunks.append(new_split)
                    else:
                        new_splits2 = self.split_text_with_seperator([',', '，'], new_split)
                        for new_split2 in new_splits2:
                            if len(self._tokenizer.encode(new_split2)) < self._chunk_config.size:
                                chunks.append(new_split2)
                            else:
                                new_splits3 = new_split2.split(' ')
                                for new_split3 in new_splits3:
                                    if len(self._tokenizer.encode(new_split3)) < self._chunk_config.size:
                                        chunks.append(new_split3)
                                    else:
                                        new_chunks = await self.split_token_with_overlap(new_split3)
                                        chunks.extend(new_chunks)
        return chunks

    def split_text_with_seperator(self, separators: List[str], content: str) -> List[str]:
        sentences = []
        tmp_sentence = ''
        for char in content:
            tmp_sentence += char
            if char in separators:
                sentences.append(tmp_sentence.strip())
                tmp_sentence = ''  
        if len(tmp_sentence) > 0:
            sentences.append(tmp_sentence.strip())
        return sentences

    async def split_token_with_overlap(self, content: str) -> List[str]:
        chunks = []
        tokens = await self._tokenizer.encode(content)
        i = 0
        while i < len(tokens):
            start = i - self._chunk_config.overlap
            if start < 0:
                start = 0
            new_tokens = tokens[start:start + self._chunk_config.size]
            i = start + self._chunk_config.size
            chunks.append(self._tokenizer.decode(content, new_tokens))
        return chunks

    def post_process(self, chunks: List[str]) -> List[str]:
        new_chunks = []
        for chunk in chunks:
            if chunk.strip():
                new_chunks.append(chunk)
        if len(new_chunks) > self._chunk_config.max_count:
            return new_chunks[:self._chunk_config.max_count]
        return new_chunks