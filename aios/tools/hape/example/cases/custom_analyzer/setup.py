# *-* coding:utf-8 *-*

import os
HERE = os.path.dirname(os.path.realpath(__file__))
DATA_DIR = os.path.realpath(os.path.join(HERE, "../../data"))


class CaseConfig:
    def __init__(self):
        self.full_data = os.path.join(DATA_DIR, "test.data")
        self.rt_data = os.path.join(DATA_DIR, "rt.data")
        self.schema = os.path.join(HERE, "in0_schema.json")
        self.queries = [
            "select in0_summary_.id, in0_summary_.title, in0_summary_.subject from in0 inner join in0_summary_ on in0.id=in0_summary_.id&&kvpair=databaseName:database;formatType:string",
            "select in0_summary_.id, in0_summary_.title, in0_summary_.subject from in0 inner join in0_summary_ on in0.id=in0_summary_.id where MATCHINDEX('default', '阿里巴巴')&&kvpair=databaseName:database;formatType:string",
            "select tfidf_score('default') from in0 where MATCHINDEX('default', '阿里巴巴')&&kvpair=databaseName:database;formatType:string",
            "select bm25_score('default') from in0 where MATCHINDEX('default', '阿里巴巴')&&kvpair=databaseName:database;formatType:string"
        ]
        
        self.hape_conf = "/ha3_install/hape_conf/default"