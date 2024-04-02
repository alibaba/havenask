# -*- coding: utf-8 -*-

from .common import *
import traceback
import click
import getpass
import re
import threading

from hape_libs.common import HapeCommon
from hape_libs.utils.shell import SSHShell
from hape_libs.appmaster.k8s.k8s_client import K8sClient


@click.command(short_help='Prepare hape necessary resources')
@common_params
def prepare(**kwargs):
    try:
        havenask_domain = command_init(kwargs)
    except Exception as e:
        Logger.error(traceback.format_exc())
        raise RuntimeError("Failed to init hape config, maybe has wrong domain config argument or failed to access zfs/hdfs/local path")
    
    
    processorMode = havenask_domain.global_config.common.processorMode
        
    if processorMode == "k8s":
        prepare_k8s_mode(havenask_domain.domain_config)
        
    Logger.info("Succeed to prepare hape necessary resources")
                

def prepare_k8s_mode(domain_config):
    global_config = domain_config.global_config
    
    binarypath, kubeconfig = global_config.common.binaryPath, global_config.common.kubeConfigPath
    client = K8sClient(binarypath, kubeconfig)
    if not client.check_connections():
        msg = "Failed to connect to k8s cluster in this machine, maybe you should check kube config in [{}]".format(kubeconfig)
        raise RuntimeError(msg)
    
    
    images = set([])
    for key in [HapeCommon.SWIFT_KEY, HapeCommon.HAVENASK_KEY, HapeCommon.BS_KEY]:
        images.add(global_config.get_appmaster_base_config(key, "image"))
        
    
    kind = "DaemonSet"
    namespace = global_config.common.k8sNamespace
    if client.read_resource(kind = "Namespace", name = namespace, namespace = None) == None:
        Logger.info("Namespace {} is not found, will create".format(namespace))
        doc = {
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {
                "annotations": {"kubectl.kubernetes.io/last-applied-configuration": 
                    '{"apiVersion":"v1","kind":"Namespace","metadata":{"annotations":{},"name":"'+ global_config.common.k8sNamespace +'"}}'},
                "name": global_config.common.k8sNamespace
            },
            "spec": {"finalizers": ["kubernetes"]}
        }
        client.create_resource(doc)
        
    
    for idx, image in enumerate(images):
        name = "havenask-image-preparer-{}".format(idx)
        daemon_set_plan = {
            "apiVersion": "apps/v1",
            "kind": kind,
            "metadata": {
                "name": name,
                "namespace" : namespace
            },
            "spec": {
                "selector": {
                    "matchLabels": {
                        "name": name
                    }
                },
                "template": {
                    "metadata": {
                        "labels": {
                        "name": name
                    }
                },
                "spec": {
                    "containers": [
                    {
                        "name": name,
                        "image": image,
                        "command": ["/bin/sh"],
                        "args": ["-c", "tail -f /dev/null"],
                        "imagePullPolicy": "Always"
                    }
                    ]
                }
                }
            }
        }
        if client.read_resource(kind = kind, name = name, namespace = namespace) != None:
            client.delete_resource(kind=kind, name=name, namespace=namespace)
            
        if client.create_resource(daemon_set_plan) == False:
            raise RuntimeError("Failed to prepare image {} by k8s daemonset".format(image))
        
    Logger.info("Succeed to papre image by daemonset in k8s cluster")