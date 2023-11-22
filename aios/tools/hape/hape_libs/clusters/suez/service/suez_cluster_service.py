import sys
import os
import requests
import json
from hape_libs.utils.logger import Logger

class SuezClusterService(object):
    def __init__(self, address):
        self._http_address = address
        Logger.info("Visit cluster service in address {}".format(self._http_address))
    
    def create_cluster(self, cluster_json):
        Logger.debug("create cluster")
        response = requests.post(self._http_address + "/ClusterService/createClusterDeployment", json={"deployment":cluster_json})
        Logger.debug(response.text)
        
    def update_cluster(self, deploy_name, cluster_json):
        Logger.debug("update cluster")
        Logger.debug("update cluster request {}".format(cluster_json))
        response = requests.post(self._http_address + "/ClusterService/updateCluster", json={"deploymentName": deploy_name, "cluster":cluster_json})
        Logger.debug(response.text)
        
    def get_cluster(self, deploy_name):
        Logger.debug("get cluster info")
        response = requests.post(self._http_address + "/ClusterService/getClusterDeployment", json={"deploymentName":deploy_name})
        Logger.debug(response.text)
        return response.json()
        
    def delete_cluster(self, clustername):
        Logger.info("begin to remove cluster {}".format(clustername))
        response = requests.post(self._http_address + "/ClusterService/deleteCluster", json={"deploymentName":"deployment", "clusterName": clustername})
        Logger.debug(response.text)
    
    def deploy_database(self, bind_json):
        Logger.debug("deploy database")
        response = requests.post(self._http_address + "/ClusterService/deployDatabase", json={
            "databaseDeployment": bind_json
        })
        Logger.debug(response.text)
        