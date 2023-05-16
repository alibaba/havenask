import os
from PyPDF2 import PdfReader
from fastapi import UploadFile
from .markdown_extractor import MarkdownExtractor

async def extract_file(file: UploadFile):
    fs = await file.read()
    temp_file_path = '/tmp/temp_file'
    with open(temp_file_path, 'wb') as fd:
        fd.write(fs)

    try:
        return extract_file_content(temp_file_path, file.filename)
    except Exception as e:
        print(f'extract file[{file.filename}], error[{str(e)}]')
        raise e
    finally:
        os.remove(temp_file_path)

def extract_file_content(file_path: str, file_name: str):
    if file_name.endswith('.pdf'):
        reader = PdfReader(file_path)
        extracted_text = " ".join([page.extract_text() for page in reader.pages])
    elif file_name.endswith('.txt') or file_name.endswith('.md'):
        with open(file_path) as fd:
            content = fd.read()
            extracted_text = MarkdownExtractor().extract(content)
    else:
        raise ValueError(f'unsupported file type, file name: {file_name}')
    return extracted_text
