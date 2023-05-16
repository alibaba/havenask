import os
import openai
import logging
from .llm import LLM
from typing import List
from tenacity import retry, wait_random_exponential, stop_after_attempt
from models.models import HAResult, ChunkConfig
from tokenizer.openai_tokenizer import OpenAITokenizer

class OpenAI(LLM):
    CHAT_ENGINE =  os.environ.get('OPENAI_CHAT_ENGINE')
    EMBEDDING_ENGINE =  os.environ.get('OPENAI_EMBEDDING_ENGINE')

    @classmethod
    def get_chunk_config(cls) -> ChunkConfig:
        return ChunkConfig(max_count=10000, size=1000, overlap=100)

    @classmethod
    def get_tokenizer(cls):
        tokenizer = OpenAITokenizer("cl100k_base")
        return tokenizer

    @classmethod
    @retry(wait=wait_random_exponential(min=1, max=20), stop=stop_after_attempt(3))
    async def embed_text(cls, text: str) -> List[float]:
        response = await openai.Embedding.acreate(input=[text], engine=cls.EMBEDDING_ENGINE)
        return [data['embedding'] for data in response['data']]

    @classmethod
    async def embed_query(cls, text: str) -> List[float]:
        return await cls.embed_text(text)

    @classmethod
    @retry(wait=wait_random_exponential(min=1, max=20), stop=stop_after_attempt(3))
    async def chat(cls, contexts: List[HAResult], query: str, history, max_length=2048, top_p=0.7, temperature=0.01):
        prompt = cls._create_prompt(contexts, query)
        chat_completion = await openai.ChatCompletion.acreate(messages=prompt, engine=cls.CHAT_ENGINE,
                                                              max_tokens=max_length, temperature=temperature, top_p=top_p)
        return chat_completion.choices[0].message.content, []

    @classmethod
    async def stream_chat(cls, contexts: List[HAResult], query: str, history, max_length=2048, top_p=0.7, temperature=0.01):
        prompt = cls._create_prompt(contexts, query)
        response = await openai.ChatCompletion.acreate(messages=prompt, engine=cls.CHAT_ENGINE, stream=True,
                                                       max_tokens=max_length, temperature=temperature, top_p=top_p)
        collected_messages = []
        async for chunk in response:
            chunk_message = chunk['choices'][0]['delta']
            collected_messages.append(chunk_message)
            result = ''.join([m.get('content', '') for m in collected_messages])
            yield result, []

    @classmethod
    def _create_prompt(cls, contexts: List[HAResult], question: str):
        context_str = ''
        for context in contexts:
            context_str += context.content
            context_str += '\n'

        system_content = f"""
            You are a question answering robot, and your goal is to try to help users get information from a given document to answer users' questions.
            Your answer should use information from the document given below, but you should not copy the content of the document verbatim.
            You need to use your own words to give an accurate, concise and clear answer. Let's think step by step.
            The given document is blow. Please answer in chinese.
            {context_str}
            """
        messages = [
            {
                "role": "system",
                "content": system_content
            },
            {
                "role": "user",
                "content": question
            }
        ]
        logging.debug(f'message: {messages}')
        return messages