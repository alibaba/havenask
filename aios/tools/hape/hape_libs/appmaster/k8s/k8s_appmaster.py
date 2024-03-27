from .k8s_client import K8sClient
from hape_libs.clusters import *
from hape_libs.appmaster import *
from hape_libs.config import DomainConfig
from hape_libs.utils.logger import Logger
from hape_libs.utils.retry import Retry

class AppMasterOnK8s(HippoAppMasterBase):
    def __init__(self, key, domain_config): #type: (str, DomainConfig) -> None
        super(AppMasterOnK8s, self).__init__(key, domain_config)
        self._k8s_client = K8sClient(self._global_config.common.binaryPath, self._global_config.common.kubeConfigPath)
    
    def start(self, master_plan):
        if not self._start_c2():
            return False
        if not self._start_appmaster(master_plan):
            return False
        return True
    
    def _start_c2(self):
        succ = self._k8s_client.create_by_yaml(self._domain_config.template_config.get_rendered_c2_plan())
        return succ
    
    def _start_appmaster(self, master_plan):
        k8splan = self._hippo_to_k8s_plan(
            master_plan, 
            self._global_config.get_appmaster_service_name(self._key),
            self._global_config.common.c2K8sNamespace
        )
        return self._k8s_client.create_resource(k8splan)
    
    def stop(self, is_delete=False):
        if not self._stop_appmasters():
            return False
        
        if not self._stop_workers():
            return False
        
        if is_delete:
            def is_all_pods_over():
                pods = self._list_pods()
                Logger.info("There are {} workernodes remaining".format(len(pods)))
                for pod in pods:
                    Logger.debug("pod info: kind: {} namespace: {} name: {}".format("WorkerNode", pod.metadata.namespace, pod.metadata.name))
                
                return len(pods) == 0
                
            succ = Retry.retry(is_all_pods_over, check_msg="All pods over {} cluster is stopped".format(self._key), 
                        limit = self._global_config.common.retryCommonWaitTimes)
            if not succ:
                Logger.error("You should delete theses resource by hand, otherwise c2 deployment will not be deleted by hape")
                return False
                
            fs_wrapper = FsWrapper(self._global_config.get_service_zk_address(self._key))
            if fs_wrapper.exists(""):
                fs_wrapper.rm("")
                
                
            
            label_selector = "app.hippo.io/cluster-name={}".format(self._global_config.common.c2ClusterName)
            all_pods_over_c2cluster = self._k8s_client.list_resources("Pod", 
                                                                      namespace = self._global_config.common.k8sNamespace, 
                                                                      label_selector=label_selector)
            if len(all_pods_over_c2cluster) == 0:
                namespace = self._global_config.common.c2K8sNamespace
                metas = [
                    ["Deployment", "c2", namespace],
                    ["Deployment", "c2-proxy", namespace],
                    ["ConfigMap", "carbon-proxy-ack-route", namespace],
                    ["Service", "c2proxy-service", namespace]
                ]
                for kind, name, namespace in metas:
                    if not self._k8s_client.delete_resource(kind=kind, name=name, namespace=namespace):
                        return False
                
        return True
    
    def _stop_appmasters(self):
        name = self._global_config.get_appmaster_service_name(self._key)
        namespace = self._global_config.common.c2K8sNamespace
        kind = "Deployment"
        def delete_appmaster():
            self._k8s_client.delete_resource(kind=kind, 
                                            name=name, 
                                            namespace=namespace)
            return self._k8s_client.read_resource(kind=kind, name=name, namespace=namespace) == None
        check_msg = "AppMaster of {} delete, kind:{} namespace:{} name:{}".format(self._key, kind, namespace, name)
        succ = Retry.retry(delete_appmaster, check_msg, limit=20)
        return succ
    
    def _stop_workers(self):
        namespace = self._global_config.common.k8sNamespace
        metas = self._global_config.get_k8s_workers_metas(self._key)
        for kind, namespace, name in metas:
            if not self._k8s_client.delete_resource(kind=kind, name=name, namespace=namespace):
                return False
        return True
    
    def _list_worker_nodes(self):
        label_key = "app.c2.io/app-name-hash"
        label_value = self._global_config.get_k8s_workers_apphash(self._key)
        workernodes = self._k8s_client.list_resources(kind="WorkerNode", 
                                                      namespace=self._global_config.common.k8sNamespace, 
                                                      label_selector=label_key+"="+label_value)
        return workernodes
        
    def get_containers_status(self): #type:() -> dict[HippoAppContainerInfo]
        annotation_key = "app.hippo.io/role-name"
        ret = {}
        try:
            pods = self._list_pods()
            for pod in pods:
                pod_name = pod.metadata.name
                host_ip = pod.status.host_ip
                pod_role = pod.metadata.annotations.get(annotation_key) if pod.metadata.annotations else None
                if pod_role == None:
                    continue
                pod_ready = all(container.ready for container in pod.status.container_statuses) if pod.status.container_statuses else False
                if pod_role not in ret:
                    ret[pod_role] = []
                ret[pod_role].append(HippoAppContainerInfo(ip=host_ip, role=pod_role, containerName=pod_name, 
                                      containerStatus=HippoContainerStatusType.RUNNING if pod_ready else HippoContainerStatusType.NOT_RUNING))
            return ret
        except Exception as e:
            Logger.warning("Get {} containers hippo app status failed".format(self._key))
            return {}
        
    def _list_pods(self):
        label_key = "app.c2.io/app-name-hash"
        label_value = self._global_config.get_k8s_workers_apphash(self._key)
        slave_pods = self._k8s_client.list_resources(kind="Pod", namespace=self._global_config.common.k8sNamespace, label_selector=label_key+"="+label_value)
        label_key = "app"
        label_value = self._global_config.get_appmaster_service_name(self._key)
        master_pods = self._k8s_client.list_resources(kind="Pod", namespace=self._global_config.common.c2K8sNamespace, label_selector=label_key+"="+label_value)
        pods = slave_pods + master_pods
        return pods
        
    def _hippo_to_k8s_plan(self, hippo_plan, service_name, namespace): #type: (HippoMasterPlan, str, str) -> dict
        process = hippo_plan.processLaunchContext.processes[0]
        image = hippo_plan.processLaunchContext.requiredPackages[0].packageURI
        args = []
        for arg in process.args:
            key, value = arg.key, arg.value
            args.append(key)
            args.append(value)
            
        envs = []
        for env in process.envs:
            key, value = env.key, env.value
            envs.append({
                "name": key,
                "value": value 
            })
            
        res_request = {
        }
        for resource in hippo_plan.resources:
            type, name, amount = resource.type, resource.name, resource.amount
            if type != "SCALAR":
                continue
            if name == "cpu":
                if amount >= 100:
                    amount = str(amount / 100.0)
                else:
                    raise ValueError("cpu amount must be int and >= 100")
            elif name == "mem":
                name = "memory"
                amount = str(amount) + "Mi"
            elif name == "disk":
                name = "storage"
                amount = str(amount) + "Mi"
            else:
                Logger.warning("resource [{}] is not supported in k8s, will ignore".format(name))
                continue
                    
            res_request[name] = amount

        volume_mounts = [
            {
               "mountPath": "/etc/localtime",
               "name": "localtime"
            }
            ,
            {
                "mountPath": "/home/admin",
                "name": "workdir"
            }
        ]
        
        volumes = [
            {
                "hostPath": {
                    "type": "",
                    "path": "/etc/localtime"
                },
                "name": "localtime"
            }
            ,
            {
                "hostPath": {
                    "type": "",
                    "path": "/home/admin/{}_appmaster".format(self.service_name)
                },
                "name": "workdir"
            }
        ]
        
        
        plan = {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "labels": {
                    "app": service_name
                },
                "name": service_name,
                "namespace": namespace,
            },
            "spec": {
                "selector": {"matchLabels": {"app": service_name}},
                "strategy": {"rollingUpdate": {"maxSurge": "50%", "maxUnavailable": "50%", "type": "RollingUpdate"}},
                "template": {
                    "metadata": {
                        "labels": {"app": service_name},
                        ## use in get_containers_status
                        "annotations": {
                            "app.hippo.io/role-name": "admin"
                        }
                    },
                    "spec": {
                        "hostNetwork": True,
                        "dnsPolicy": "ClusterFirstWithHostNet",
                        "containers":[
                            {
                                "args": args,
                                "env": envs,
                                "command": [process.cmd],
                                "image": image,
                                # "securityContext":{
                                #     "runAsUser": 1000,
                                #     "runAsGroup": 1000,
                                #     "fsGroup": 1000
                                # },
                                "workingDir": "/home/admin",
                                "imagePullPolicy": "Always",
                                "name": service_name,
                                "resources": {"requests": res_request},
                                "volumeMounts": volume_mounts
                            }
                        ],
                        "volumes": volumes
                    }
                }
            }
        }
        return plan

