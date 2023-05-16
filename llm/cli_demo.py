import inspect
import asyncio
from dotenv import load_dotenv
load_dotenv()

import logging
logging.basicConfig(format="[%(asctime)s] [%(levelname)s] %(message)s", level=logging.WARNING)

from llm_adapter.factory import get_llm_adapter
from havenask.havenask import Havenask

async def run():
    print("Welcome to Havenask LLM demo!!")
    llm = get_llm_adapter()
    history = []
    while True:
        query = input("\nUser: ")
        if query.strip() == "exit":
            break
        elif query.strip() == "clear":
            history = []
            continue

        embedding = await llm.embed_query(query)
        candidate_docs = await Havenask.query(embedding, 3)

        print("AI: ", end='', flush=True)
        last_print_len = 0

        if inspect.iscoroutinefunction(llm.stream_chat):
            response = await llm.stream_chat(candidate_docs, query, history)
        else:
            response = llm.stream_chat(candidate_docs, query, history)

        async for result, history in response:
            print(f"{result[last_print_len:]}", end='', flush=True)
            last_print_len = len(result)
        print('')
        print('-------')
        print(f'Source: {[doc.source_id for doc in candidate_docs]}')
        print('-------')

if __name__ == "__main__":
    asyncio.run(run())
