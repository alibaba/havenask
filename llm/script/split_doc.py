import os
import sys
import asyncio
from  dotenv import load_dotenv
load_dotenv()

from models.models import (
    DocumentList,
    HADocument
)
from llm_adapter.factory import get_llm_adapter
from spliter.sentence_spliter import SentenceSpliter

CUR_DIR = os.path.split(os.path.realpath(__file__))[0]

async def split_doc(request: DocumentList, output_file: str):
    spliter = SentenceSpliter()
    fd = open(output_file, 'w')
    for doc in request.documents:
        chunks = await spliter.split_token_with_overlap(doc.content)
        embeddings = []
        for chunk in chunks:
            embedding = await get_llm_adapter().embed_text([chunk])
            embeddings.append(embedding[0])
        for i, chunk in enumerate(chunks):
            ha_document = HADocument(id=i, source_id=doc.id, content=chunk, embedding=embeddings[i])
            fd.write(ha_document.to_ha_str())
    fd.close()

if __name__ == '__main__':
    doc_path = sys.argv[1]
    output_path = sys.argv[2]
    request = DocumentList.parse_file(doc_path)
    asyncio.run(split_doc(request, output_path))
