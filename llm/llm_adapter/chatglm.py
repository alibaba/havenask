import os
from .llm import LLM
from .local_chatglm import LocalChatGLM
from .remote_chatglm import RemoteChatGLM
from embedding.local_embedding import LocalEmbedding
from embedding.remote_embedding import RemoteEmbedding
from typing import List
from models.models import HAResult, ChunkConfig

CUR_DIR = os.path.split(os.path.realpath(__file__))[0]

class ChatGLM(LLM):
    CHATGLM_ENDPOINT = os.environ.get('CHATGLM_SERVER_ENDPOINT')
    EMBEDDING_ENDPOINT = os.environ.get('EMBEDDING_SERVER_ENDPOINT')

    @classmethod
    def get_chunk_config(cls):
        return ChunkConfig(max_count=50000, size=300, overlap=50)

    @classmethod
    def get_tokenizer(cls):
        return cls.get_embedding_instance().get_tokenizer()

    @classmethod
    async def embed_text(cls, text: str) -> List[float]:
        return await cls.get_embedding_instance().embed_text(text)

    @classmethod
    async def embed_query(cls, text: str) -> List[float]:
        return await cls.get_embedding_instance().embed_query(text)

    @classmethod
    async def chat(cls, contexts: List[HAResult], query: str, history: List[List[str]], max_length=2048, top_p=0.7, temperature=0.01):
        prompt = cls._create_prompt(contexts, query)
        return await cls.get_chat_instance().chat(prompt, history, max_length=max_length, top_p=top_p, temperature=temperature)

    @classmethod
    async def stream_chat(cls, contexts: List[HAResult], query: str, history: List[List[str]], max_length=2048, top_p=0.7, temperature=0.01):
        prompt = cls._create_prompt(contexts, query)
        return cls.get_chat_instance().stream_chat(prompt, history, max_length=max_length, top_p=top_p, temperature=temperature)

    @classmethod
    def _create_prompt(cls, contexts: List[HAResult], question: str):
        context_str = ''
        for context in contexts:
            context_str += 'Content: ' + context.content
            context_str += '\n'
        return f"""已知信息：
                {context_str} 

                根据上述已知信息，简洁和专业的来回答用户的问题。如果无法从中得到答案，请说不知道，不允许在答案中添加编造成分，答案请使用中文。
                问题是：{question}"""

    @classmethod
    def get_embedding_instance(cls):
        return RemoteEmbedding.get_instance() if cls.EMBEDDING_ENDPOINT else LocalEmbedding.get_instance()

    @classmethod
    def get_chat_instance(cls):
        return RemoteChatGLM.get_instance() if cls.CHATGLM_ENDPOINT else LocalChatGLM.get_instance()