from .appmaster_base import * 
from .proc.proc_appmaster import AppMasterOnLocalProcessor
from .k8s.k8s_appmaster import AppMasterOnK8s
from .docker.docker_appmaster import AppMasterOnDocker
from .appmaster_hippo_common import *

HippoAppMasterBase.register_master_cls("proc", AppMasterOnLocalProcessor)
HippoAppMasterBase.register_master_cls("k8s", AppMasterOnK8s)
HippoAppMasterBase.register_master_cls("docker", AppMasterOnDocker)