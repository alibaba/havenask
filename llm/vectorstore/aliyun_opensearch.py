import os
import json
import logging
from typing import List
from models.models import HAResult, HADocument
from alibabacloud_ha3engine import models, client
from Tea.exceptions import TeaException, RetryError
from .basestore import BaseStore

class AliyunOpenSearch(BaseStore):
    ENDPOINT = os.environ.get('OPENSEARCH_ENDPOINT')
    INSTANCE_ID = os.environ.get('OPENSEARCH_INSTANCE_ID')
    TABLE_NAME = os.environ.get('OPENSEARCH_TABLE_NAME')
    DATA_SOURCE = os.environ.get('OPENSEARCH_DATA_SOURCE')
    USERNAME = os.environ.get('OPENSEARCH_USER_NAME')
    PASSWORD = os.environ.get('OPENSEARCH_PASSWORD')

    def __init__(self) -> None:
        self.config = models.Config(
            endpoint=self.ENDPOINT,
            instance_id=self.INSTANCE_ID,
            protocol="http",
            access_user_name=self.USERNAME,
            access_pass_word=self.PASSWORD
        )

    async def insert(self, ha_docs: List[HADocument]) -> None:
        engine_client = client.Client(self.config)
        options_headers = {}

        pk_field = 'pk'
        documents = [ha_doc.to_os_doc() for ha_doc in ha_docs]
        request = models.PushDocumentsRequestModel(options_headers, documents)
        await engine_client.push_documents_async(self.DATA_SOURCE, pk_field, request)

    async def query(self, embedding: List[float], topn: int) -> List[HAResult]:
        engine_client = client.Client(self.config)
        option_headers = {}

        result = []
        try:
            embedding_str = ','.join('{:.6f}'.format(fp) for fp in embedding)
            sql = f'''select pk, content, source_id, proxima_score('embedding_index') as score from {self.TABLE_NAME}
                      where MATCHINDEX('embedding_index', '{embedding_str}&n={topn}') order by score asc limit 100&&kvpair=format:full_json'''
            search_query = models.SearchQuery(sql=sql)
            search_request = models.SearchRequestModel(option_headers, search_query)
            search_response = await engine_client.search_async(search_request)

            obj = json.loads(search_response.body)
            error_info = obj['error_info']
            if error_info['ErrorCode'] != 0:
                raise RuntimeError(f"query opensearch failed, {error_info['Message']}")
            data = obj['sql_result']['data']
            result = [HAResult(pk=column[0], content=column[1], source_id=column[2]) for column in data]
            return result
        except Exception as e:
            logging.exception(f"search opensearch failed: {e}")
        finally:
            return result