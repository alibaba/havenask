import sys
import asyncio
from  dotenv import load_dotenv
load_dotenv()
from pathlib import Path
from typing import List
from models.models import HADocument
from spliter.sentence_spliter import SentenceSpliter
from llm_adapter.factory import get_llm_adapter
from extractor.file_extractor import extract_file_content

async def parse_dir(filepath: Path, ignore_dirs: List[str], spliter: SentenceSpliter, outfile):
    if filepath.is_dir():
        if filepath.name in ignore_dirs:
            print(f'ignore {filepath.name}')
            return
        for sub_file in filepath.iterdir():
            await parse_dir(sub_file, ignore_dirs, spliter, outfile)
    else:
        print(f'extract {filepath.absolute()}')
        try:
            file_content = extract_file_content(filepath, filepath.name)
        except ValueError as e:
            print(f'ignore unsupported file {filepath.name}')
            return
        if len(file_content) < 10:
            print(f'ignore short file {filepath.name}')
            return

        chunks = await spliter.split_token_with_overlap(file_content)
        embeddings = []
        for chunk in chunks:
            embedding = await get_llm_adapter().embed_text(chunk)
            embeddings.append(embedding)
        for i, chunk in enumerate(chunks):
            ha_document = HADocument(id=i, source_id=filepath.name, content=chunk, embedding=embeddings[i])
            outfile.write(ha_document.to_ha_str())
    
if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('Usage: python embed_fiels.py input_dir output_file_name ignore_dir')
        sys.exit(-1)

    input_dir = sys.argv[1]
    output_file_name = sys.argv[2]
    ignore_dirs = []
    if len(sys.argv) > 3:
        ignore_dirs = sys.argv[3].split(',')

    input_path = Path(input_dir)
    spliter = SentenceSpliter()
    with open(output_file_name, 'w') as fd:
        asyncio.run(parse_dir(input_path, ignore_dirs, spliter, fd))