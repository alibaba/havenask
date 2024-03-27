import sys
import os
import requests
import traceback
import json
import attr


from hape_libs.utils.logger import Logger
from hape_libs.clusters.cluster_base import ShardGroupProcessorStatus, ProcessorStatusType

class SchedulerService(object):
    def __init__(self, address):
        self._http_address = address
        Logger.info("visit scheduler service in address {}".format(self._http_address))

    def get_processors_status(self):
        Logger.debug("get scheduler status of havenask admin")
        response = requests.post(self._http_address + "/SchedulerService/getSystemInfo")
        Logger.debug(response.text)
        sys_info_str = response.json()["systemInfoStr"]
        result = self.extract_processors_status(sys_info_str)
        return result

    def extract_processors_status(self, system_info_str):
        status_map = {}
        try:
            system_info_dict = json.loads(system_info_str)
            for group in system_info_dict:
                role_statuses = system_info_dict[group]['roleStatuses']
                status_map[group] = {}
                for role in role_statuses:
                    replica_nodes_status = role_statuses[role]['nextInstanceInfo']['replicaNodes']
                    role_status_list = self.extract_replica_nodes_status(role, replica_nodes_status)
                    status_map[group][role] = role_status_list
                    
        except:
            Logger.error("Failed to extract processors status")
            Logger.error(traceback.format_exc())
        return status_map

    def extract_replica_nodes_status(self, role, replica_nodes_status):
        status_list = []
        for node_status in replica_nodes_status:
            processor_status = ShardGroupProcessorStatus()
            processor_status.workerStatus = node_status["workerStatus"]
            if len(node_status["processStatus"]) == 1:
                processor_status.processorStatus = ProcessorStatusType.RUNNING if node_status["processStatus"][0]["status"] == "PS_RUNNING" else ProcessorStatusType.NOT_RUNNING
            processor_status.replia_id = node_status["replicaNodeId"]
            processor_status.signature = node_status["signature"]
            processor_status.targetSignature = node_status["targetSignature"]
            processor_status.readyForCurVersion = node_status["readyForCurVersion"]
            processor_status.ip = node_status["ip"]
            processor_status.role = role
            status_list.append(processor_status)
        return status_list
