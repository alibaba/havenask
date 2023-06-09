import sys
import argparse
import asyncio
from  dotenv import load_dotenv
load_dotenv()
from pathlib import Path
from typing import List
from models.models import HADocument
from spliter.sentence_spliter import SentenceSpliter
from llm_adapter.factory import get_llm_adapter
from vectorstore.factory import get_vector_store
from extractor.file_extractor import extract_file_content

llm = get_llm_adapter()
store = get_vector_store()

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
            embedding = await llm.embed_text(chunk)
            embeddings.append(embedding)
        for i, chunk in enumerate(chunks):
            ha_document = HADocument(id=i, source_id=filepath.name, content=chunk, embedding=embeddings[i])
            if outfile:
                outfile.write(ha_document.to_ha_str())
            else:
                await store.insert([ha_document])
    
if __name__ == '__main__':
    opt_parser = argparse.ArgumentParser(description="run app")
    opt_parser.add_argument("-f", dest="input_path", help="input file or dir", required=True)
    opt_parser.add_argument("-o", dest="output", help="[option] output file path", required=False)
    opt_parser.add_argument("-i", dest="ignore_dirs", help="[option] ignore dirs", required=False)
    args, _ = opt_parser.parse_known_args(sys.argv[1:])

    input_dir = args.input_path
    output_file_name = args.output
    ignore_dirs = []
    if args.ignore_dirs:
        ignore_dirs = args.ignore_dirs.split(',')

    input_path = Path(input_dir)
    spliter = SentenceSpliter()
    fd = None
    if output_file_name:
        fd = open(output_file_name, 'w')
    asyncio.run(parse_dir(input_path, ignore_dirs, spliter, fd))