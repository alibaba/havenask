from argparse import ArgumentParser
import time
import os
import sys
import json
hape_path = os.path.abspath(os.path.join(os.path.abspath(__file__), "../../../../"))
sys.path.append(hape_path)


from hape.utils.dict_file_util import DictFileUtil
from hape.utils.logger import Logger

from hape.domains.workers.roles.bs_target_worker import BsTargetWorker
from hape.domains.workers.roles.searcher_target_worker import SearcherTargetWorker
from hape.domains.workers.roles.qrs_target_worker import QrsTargetWorker


class HaTargetWorkerFactory:
    @staticmethod
    def create(global_conf, domain_name, role_name, container_name):
        if role == "bs":
            return BsTargetWorker(global_conf, domain_name, role_name, container_name)
        if role == "qrs":
            return QrsTargetWorker(global_conf, domain_name, role_name, container_name).watch()
        if role == "searcher":
            return SearcherTargetWorker(global_conf, domain_name, role_name, container_name).watch()
    
if __name__  == "__main__":
    parser =  ArgumentParser()
    parser.add_argument("--domain_name", required=True)
    parser.add_argument("--role_name", required=True)
    parser.add_argument("--worker_name", required=True)
    parser.add_argument("--global_conf", required=True)
    args = parser.parse_args()
    role = args.role_name
    worker_name = args.worker_name
    domain_name = args.domain_name
    global_conf = DictFileUtil.read(args.global_conf)
    Logger.init(global_conf["hape-worker"]["logging-conf"])
    Logger.logger_name = "worker"
    HaTargetWorkerFactory.create(global_conf, domain_name, role, worker_name).watch()
        