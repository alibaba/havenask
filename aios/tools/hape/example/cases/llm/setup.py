# *-* coding:utf-8 *-*

import os
HERE = os.path.dirname(os.path.realpath(__file__))
DATA_DIR = os.path.realpath(os.path.join(HERE, "../../data"))


class CaseConfig:
    def __init__(self):
        self.full_data = os.path.join(DATA_DIR, "llm.data")
        self.rt_data = None
        self.schema = os.path.join(HERE, "in0_schema.json")
        self.queries = [
        ]
        self.hape_conf = os.path.join(HERE, "llm_conf")
