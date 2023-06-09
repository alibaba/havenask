import os
import json
import aiohttp
import logging
import urllib.parse
from typing import List
from models.models import HAResult, HADocument
from .basestore import BaseStore

class Havenask(BaseStore):
    HA_QRS_ADDRESS = os.environ.get('HA_QRS_ADDRESS', '127.0.0.1:45800')
    HA_TABLE_NAME = os.environ.get('HA_TABLE_NAME', 'embedding')

    async def insert(self, docs: List[HADocument]):
        raise NotImplementedError

    async def query(self, embedding: List[float], topn: int) -> List[HAResult]:
        result = []
        embedding_str = ','.join([str(fp) for fp in embedding])
        sql = f'''select pk, content, source_id from {self.HA_TABLE_NAME} where MATCHINDEX('embedding_index', '{embedding_str}&n={topn}')&&kvpair=format:full_json'''
        qrs_address = self.HA_QRS_ADDRESS
        if not qrs_address.startswith('http://'):
            qrs_address = 'http://' + qrs_address
        url = f'{qrs_address}/sql?{urllib.parse.quote(sql)}'
        logging.debug(f'query havenask: {url}')

        async with aiohttp.ClientSession(raise_for_status=True) as session:
            async with session.get(url) as response:
                text = await response.text()
                logging.debug(f'query havenask: {url}, result: {text}')

                obj = json.loads(text)
                error_info = obj['error_info']
                if error_info['ErrorCode'] != 0:
                    raise RuntimeError(f"query havenask failed, {error_info['Message']}")
                data = obj['sql_result']['data']
                result = [HAResult(pk=column[0], content=column[1], source_id=column[2]) for column in data]
        return result