# -*- coding: utf-8 -*-

import io
import traceback
import json
import functools
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


from hape_libs.clusters.cluster_base import *
from hape_libs.utils.logger import Logger
from hape_libs.utils.retry import Retry


class K8sClient():
    def __init__(self, binary_path, kube_config_path):
        ## 暂时解决bazel下python2对于yaml库以及k8s内的yaml库兼容性问题，引入lib/yaml
        import sys
        sys.path = [binary_path + "/usr/local/lib/python/site-packages/lib"] + sys.path
        import lib.yaml as yaml
        import kubernetes as k8s
        self.yaml = yaml
        self.k8s = k8s
        self.k8s.config.load_kube_config(kube_config_path)
        configuration = self.k8s.client.Configuration().get_default_copy()
        configuration.verify_ssl = False
        api_client = self.k8s.client.ApiClient(configuration=configuration)
        self._core_v1 = self.k8s.client.CoreV1Api(api_client)
        self._rbac_v1 = self.k8s.client.RbacAuthorizationV1Api(api_client)
        self._apps_v1 = self.k8s.client.AppsV1Api(api_client)
        self._apiextensions_v1 = self.k8s.client.ApiextensionsV1Api(api_client)
        self._custom_objects_api = self.k8s.client.CustomObjectsApi(api_client)
        self._priorityclass_api = self.k8s.client.SchedulingV1Api(api_client)

        self._resources_api_map = {
            "CustomResourceDefinition": {
                "read": self._apiextensions_v1.read_custom_resource_definition,
                "create": self._apiextensions_v1.create_custom_resource_definition,
                "delete": self._apiextensions_v1.delete_custom_resource_definition,
                "namespace_required": False
            },
            "Namespace": {
                "read": self._core_v1.read_namespace,
                "create": self._core_v1.create_namespace,
                "delete": self._core_v1.delete_namespace,
                "namespace_required": False
            },
            "PriorityClass": {
                "read": self._priorityclass_api.read_priority_class,
                "create": self._priorityclass_api.create_priority_class,
                "delete": self._priorityclass_api.delete_priority_class,
                "namespace_required": False
                
            },
            "ServiceAccount": {
                "read": self._core_v1.read_namespaced_service_account,
                "create": self._core_v1.create_namespaced_service_account,
                "delete": self._core_v1.delete_namespaced_service_account,
                "namespace_required": True
            },
            "ClusterRoleBinding": {
                "read": self._rbac_v1.read_cluster_role_binding,
                "create": self._rbac_v1.create_cluster_role_binding,
                "delete": self._rbac_v1.delete_cluster_role_binding,
                "namespace_required": False
            },
            "Secret": {
                "read": self._core_v1.read_namespaced_secret,
                "create": self._core_v1.create_namespaced_secret,
                "delete": self._core_v1.delete_namespaced_secret,
                "namespace_required": True
            },
            "Deployment": {
                "read": self._apps_v1.read_namespaced_deployment,
                "create": self._apps_v1.create_namespaced_deployment,
                "delete": self._apps_v1.delete_namespaced_deployment,
                "list": self._apps_v1.list_namespaced_deployment,
                "namespace_required": True
            },
            "Pod": {
                "list": self._core_v1.list_namespaced_pod,
                "namespace_required": True
            },
            "Service": {
                "read": self._core_v1.read_namespaced_service,
                "create": self._core_v1.create_namespaced_service,
                "delete": self._core_v1.delete_namespaced_service,
                "namespace_required": True
            },
            "ConfigMap": {
                "read": self._core_v1.read_namespaced_config_map,
                "create": self._core_v1.create_namespaced_config_map,
                "delete": self._core_v1.delete_namespaced_config_map,
                "namespace_required": True
            },
            "ShardGroup": {
                "read": functools.partial(self._custom_objects_api.get_namespaced_custom_object, group="carbon.taobao.com", version="v1", plural="shardgroups"),
                "delete": functools.partial(self._custom_objects_api.delete_namespaced_custom_object, group="carbon.taobao.com", version="v1", plural="shardgroups"),
                "list": functools.partial(self._custom_objects_api.list_namespaced_custom_object, group="carbon.taobao.com", version="v1", plural="shardgroups"),
                "namespace_required": True
            },
            "CarbonJob": {
                "read": functools.partial(self._custom_objects_api.get_namespaced_custom_object, group="carbon.taobao.com", version="v1", plural="carbonjobs"),
                "delete": functools.partial(self._custom_objects_api.delete_namespaced_custom_object, group="carbon.taobao.com", version="v1", plural="carbonjobs"),
                "list": functools.partial(self._custom_objects_api.list_namespaced_custom_object, group="carbon.taobao.com", version="v1", plural="carbonjobs"),
                "namespace_required": True
            },
            "WorkerNode": {
                "list": functools.partial(self._custom_objects_api.list_namespaced_custom_object, group="carbon.taobao.com", version="v1", plural="workernodes"),
                "namespace_required": True
            },
            "DaemonSet": {
                "read":   self._apps_v1.read_namespaced_daemon_set,
                "create": self._apps_v1.create_namespaced_daemon_set,
                "delete": self._apps_v1.delete_namespaced_daemon_set,
                "namespace_required": True
            }
        }
            
    def create_by_yaml(self, content):
        docs = self.yaml.safe_load_all(content)
        for doc in docs:
            if not self.create_resource(doc):
                return False
        return True
                
    def read_resource(self, kind, name, namespace):
        if name == None:
            Logger.error("K8s resource requires name".format(name))
            return None
        
        if kind == None:
            Logger.error("K8s resource {} requires kind".format(kind))
            return None
        
        if kind not in self._resources_api_map:
            Logger.error("K8s resource {} with kind {} is not supported by hape by now, you can create by hand".format(name, kind))
            return None
        
        try:
            resource_api = self._resources_api_map[kind]
            method, ns_required = resource_api["read"], resource_api["namespace_required"]
            if ns_required and namespace == None:
                Logger.error("K8s resource {} requires namespace".format(name))
                return None
            if ns_required:
                obj = method(name=name, namespace=namespace)
            else:
                obj = method(name=name)
            return obj
        except Exception as e:
            Logger.debug(traceback.format_exc())
            return None
        
    def list_resources(self, kind, namespace, label_selector = None):
        if kind not in self._resources_api_map:
            Logger.error("K8s resource kind {} not supported by hape, you can list by hand".format(kind))
            return []
        resource_api = self._resources_api_map[kind]
        method = resource_api["list"]
        if label_selector != None:
            objs = method(namespace=namespace, label_selector = label_selector)
        else:
            objs = method(namespace=namespace)
        return objs.items
        
    def create_resource(self, doc):
        metadata = doc.get("metadata", {})
        name = metadata.get("name", None)
        namespace = metadata.get("namespace", None)
        kind = doc.get("kind", None)
        metainfo = "kind: {}, name: {} namespace: {}".format(kind, name, namespace)
        if self.read_resource(kind, name, namespace) != None:
            Logger.info("K8s resource already exists, kind:{} namespace:{} name:{}".format(kind, namespace, name))
            return True
        Logger.info("K8s resource not exists, will create it, {}".format(name, metainfo))
        Logger.debug("K8s resource {} spec: {}".format(name, self.yaml.dump(doc)))
        resource_api = self._resources_api_map[kind]
        method, ns_required = resource_api["create"] , resource_api['namespace_required']
        try:
            if not ns_required:
                method(body=doc)
            else:
                method(body=doc, namespace=namespace)
            Logger.info("Create k8s resource succeed, {}".format(metainfo))
            return True
        except Exception as e:
            Logger.error("Failed to create k8s resource, {}".format(metainfo))
            Logger.error(traceback.format_exc())
            return False
    
    def delete_resource(self, kind, name, namespace = None):
        metainfo = "kind: {}, namespace: {}, name: {}".format(kind, namespace, name)
        if kind not in self._resources_api_map:
            Logger.error("K8s resource kind {} not supported by hape, you can delete by hand".format(kind))
            return False
        
        if not self.read_resource(kind, name, namespace) != None:
            return True
        
        Logger.info("Begin to delete K8s resource, {}".format(metainfo))
        try:
            resource_api = self._resources_api_map[kind]
            ns_required, method = resource_api["namespace_required"], resource_api["delete"]
            if ns_required:
                if namespace == None:
                    Logger.error("Failed to delete k8s resource, namespace is required, {}".format(metainfo))
                    return False
                method(namespace=namespace,name=name)
            else:
                method(name=name)
            Logger.info("K8s resource deleted successfully, {}".format(metainfo))
            
            def check_resource_deleted():
                return self.read_resource(kind=kind, name=name, namespace=namespace) == None
            succ = Retry.retry(check_resource_deleted, check_msg="K8s resource deleted, {}".format(metainfo), limit=20)
            return succ
        except Exception as e:
            Logger.error("Failed to delete k8s resource, {}".format(metainfo))
            Logger.error("You can delete it by hand")
            Logger.error(traceback.format_exc())
            return False
        
    def check_connections(self):
        try:
            api_versions = self._core_api.get_api_versions()
            Logger.info("Connected to Kubernetes cluster")
            return True
        except Exception as e:
            Logger.error("Failed to connect to Kubernetes cluster:", e)
            Logger.error(traceback.format_exc())
            return False