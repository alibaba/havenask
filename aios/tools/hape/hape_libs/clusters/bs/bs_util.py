from math import e
import sys
import os
import requests
import traceback
import json
import attr
from distutils.command import build


from hape_libs.utils.logger import Logger
from hape_libs.clusters.cluster_base import *

class BsAdminService(object):
    def __init__(self, address):
        self._http_address = address
        Logger.info("visit bs admin service in address {}".format(self._http_address))

    def get_processors_status(self):
        Logger.debug("get scheduler status of havenask admin")
        response = requests.post(self._http_address + "/AdminService/getServiceInfo")
        Logger.debug(response.text)
        response = response.json()
        status_map = {}
        for generation_info in response.get("generationInfos", []):
            if not 'buildInfo' in generation_info:
                continue
            
            partitions = []
            if 'processorInfo' in generation_info:
                partitions.extend(generation_info['processorInfo'].get('partitionInfos', []))
                
            if "buildInfo" in generation_info and "clusterInfos" in generation_info['buildInfo']:
                cluster_infos = generation_info['buildInfo']['clusterInfos']
                for cluster_info in cluster_infos:
                    if 'builderInfo' in cluster_info:
                        partitions.extend(cluster_info['builderInfo'].get('partitionInfos',[]))
                    if 'mergerInfo'  in cluster_info:
                        partitions.extend(cluster_info['mergerInfo'].get('partitionInfos',[]))
                        
            for partition_info in partitions:
                if 'longAddress' not in partition_info['currentStatus']:
                    continue
                ip = partition_info['currentStatus']['longAddress']['ip']
                pid = partition_info["pid"]
                from_, to_ = pid["range"]["from"], pid["range"]["to"]
                buildid = pid["buildId"]
                taskid = pid["taskId"]
                app, table, generation = buildid['appName'], buildid['dataTable'], buildid['generationId']
                if pid["role"] == "ROLE_PROCESSOR":
                    step = "inc" if pid["step"] == 'BUILD_STEP_INC' else 'full'
                    role = "{}.{}.{}.processor.{}.{}.{}.{}".format(app, table, generation, step, from_, to_, taskid)
                else:
                    role = "{}.{}.{}.task.{}.{}.{}.{}".format(app, table, generation, taskid, from_, to_, table)
                status = ProcessorStatusType.RUNNING
                if role not in status_map:
                    status_map[role] = []
                status_map[role].append(ClusterProcessorStatus(ip=ip, role=role, processorStatus=status))
        return status_map
            
            
