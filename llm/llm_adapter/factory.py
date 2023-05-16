import os
import logging
from .openai import OpenAI
from .chatglm import ChatGLM

def get_llm_adapter():
    llm_name = os.environ.get('LLM_NAME')
    if llm_name == 'OpenAI':
        logging.info('Use OpenAI')
        return OpenAI
    elif llm_name == 'ChatGLM':
        logging.info('Use ChatGLM')
        return ChatGLM
    else:
        raise RuntimeError(f'unknown llm: {llm_name}')