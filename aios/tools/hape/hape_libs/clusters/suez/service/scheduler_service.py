import sys
import os
import requests
import json
from hape_libs.utils.logger import Logger

class SchedulerService(object):
    def __init__(self, address):
        self._http_address = address
        Logger.info("visit scheduler service in address {}".format(self._http_address))

    def get_status(self, detail=False):
        Logger.debug("get status")
        response = requests.post(self._http_address + "/SchedulerService/getSystemInfo")
        Logger.debug(response.text)
        sys_info_str = response.json()["systemInfoStr"]
        result = {}
        simple_status = self.extract_simple_status(sys_info_str)
        generation_ids = self.extract_generation_ids(sys_info_str)
        if not detail:
            result = simple_status
        else:
            result = json.loads(sys_info_str)
            result["clusterStatus"] = simple_status["clusterStatus"]
            result["GenerationIds"] = generation_ids
        return result

    def extract_simple_status(self, system_info_str):
        simple_status = {}
        try:
            system_info_dict = json.loads(system_info_str)
            all_ready = True
            qrs_role_statuses = system_info_dict['qrs']['roleStatuses']['qrs_partition_0']['nextInstanceInfo']['replicaNodes']
            is_all_qrs_ready, status = self.extract_replica_nodes_status(qrs_role_statuses)
            all_ready = all_ready and is_all_qrs_ready
            simple_status["qrs"] = status

            simple_status["database"] = {}
            db_role_statuses = system_info_dict['database']['roleStatuses']
            for db_role in db_role_statuses:
                replica_nodes_status = db_role_statuses[db_role]['nextInstanceInfo']['replicaNodes']
                is_all_searcher_ready, status = self.extract_replica_nodes_status(replica_nodes_status)
                simple_status["database"][db_role] = status
                all_ready = all_ready and is_all_searcher_ready
            simple_status["clusterStatus"] =  "READY" if all_ready else "NOT_READY"
        except:
            simple_status["clusterStatus"] = "UNKNOWN"
        return simple_status

    def extract_replica_nodes_status(self, replica_nodes_status):
        status = []
        is_all_ready = (len(replica_nodes_status) > 0)
        for node_status in replica_nodes_status:
            simple_node_status =  {}
            simple_node_status["serviceStatus"] = node_status["serviceStatus"]
            simple_node_status["healthStatus"] = node_status["healthStatus"]
            simple_node_status["workerStatus"] = node_status["workerStatus"]
            simple_node_status["slotId"] = node_status["slotId"]
            simple_node_status["replicaNodeId"] = node_status["replicaNodeId"]
            status.append(simple_node_status)
            is_all_ready = (simple_node_status["serviceStatus"] == "SVT_AVAILABLE") and (simple_node_status["healthStatus"] == "HT_ALIVE") and (simple_node_status["workerStatus"] == "WS_READY")
        return is_all_ready, status

    def extract_generation_ids(self, system_info_str):
        generation_ids = set()
        try:
            system_info_dict = json.loads(system_info_str)
            all_ready = True
            searcher_role_statuses = system_info_dict['database']['roleStatuses']
            for searcher_role in searcher_role_statuses:
                replica_nodes_status = searcher_role_statuses[searcher_role]['nextInstanceInfo']['replicaNodes']
                for node_status in replica_nodes_status:
                    generation_id = node_status['signature'].split('.')[1]
                    generation_ids.add(int(generation_id))
        except:
            generation_ids = set()
        return list(generation_ids)
