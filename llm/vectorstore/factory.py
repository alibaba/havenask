
import os
import logging
from .havenask import Havenask
from .aliyun_opensearch import AliyunOpenSearch

def get_vector_store():
    store_name = os.environ.get('VECTOR_STORE')
    if store_name == 'Havenask':
        logging.info('Use Havenask')
        return Havenask()
    elif store_name == 'OpenSearch':
        logging.info('Use OpenSearch')
        return AliyunOpenSearch()
    else:
        raise RuntimeError(f'unknown vector store: {store_name}')