# -*- coding: utf-8 -*-

from platform import processor
from .common import *
import traceback
import click
import getpass
import re

from hape_libs.utils.logger import Logger
from hape_libs.utils.shell import SSHShell
from hape_libs.common import HapeCommon
from hape_libs.appmaster.k8s.k8s_client import *
from hape_libs.appmaster.docker.docker_util import *


@click.command(short_help='Validate hape env and domain conf')
@common_params
def validate(**kwargs):
    try:
        havenask_domain = command_init(kwargs)
    except Exception as e:
        Logger.error(traceback.format_exc())
        raise RuntimeError("Failed to init hape config, maybe has wrong domain config argument or failed to access zfs/hdfs/local path")
    
    
    processorMode = havenask_domain.global_config.common.processorMode
    if processorMode == "docker":
        validate_docker_mode(havenask_domain.domain_config)
        
    if processorMode == "k8s":
        validate_k8s_mode(havenask_domain.domain_config)
        
                
    # validate_port(domain_config)
    Logger.info("Succeed to validate basic hape enviroment")

def validate_user():
    user = getpass.getuser()
    not_supported_users = ['root']
    if user in not_supported_users:
        raise RuntimeError("account:{} is not supported, please use anothor acount".format(user))
    
    home = os.path.expanduser("~")
    if not home.startswith("/home"):
        raise RuntimeError("please make sure account:{} has home directory".format(user))
    
def validate_single(key, ip, domain_config):
    Logger.info("Begin to validate single container")
    
    global_config = domain_config.global_config
    ## check if image exists and container can be created
    image = global_config.get_appmaster_base_config(key, "image")
    container_name = "havenask-validate-single"
    
    shell = SSHShell(ip)
    out, succ = shell.execute_command("docker images | grep {}".format(".*".join(image.split(":"))), grep_text=image.split(":")[0])
    if not succ:
        raise RuntimeError("Image {} not found in ip {}".format(image, ip))
    
    homedir, user = global_config.default_variables.user_home, global_config.default_variables.user
    workdir = os.path.join(homedir, container_name)
    succ = DockerContainerUtil.create_container(ip=ip, name=container_name, workdir= workdir, homedir = homedir, user=user, cpu=100, mem=5120, image=image)
    if not succ:
        raise RuntimeError("Failed to create container")
    
    DockerContainerUtil.stop_container(ip=ip, name=container_name)
    Logger.info("Succeed to validate single container")
    
    
## check each admin can schedule workers
def validate_schedule(key, admin_ip, worker_ip, domain_config):
    Logger.info("Begin to schedule from {} to {}".format(admin_ip, worker_ip))
    container_name = "havenask-validate-schedule"
    shell = SSHShell(admin_ip)
    global_config = domain_config.global_config
    user_home = global_config.default_variables.user_home
    image = global_config.get_appmaster_base_config(key, "image")
    run_cmd = "docker run --workdir {} --volume=\"/etc/group:/etc/group:ro\" --volume=\"/etc/passwd:/etc/passwd:ro\"  --volume=\"/etc/shadow:/etc/shadow:ro\" -v {}:{} --ulimit nofile=655350:655350  \
--ulimit memlock=-1 --ulimit core=-1 --network=host --privileged -d  --cpu-quota=500000 \
--cpu-period=100000 --memory=5124m -v /etc/passwd:/home/.passwd -v /etc/group:/home/.group --name {} {} /sbin/init".format(user_home, user_home, user_home, container_name, image)
    cmd = "ssh -o PasswordAuthentication=no -o StrictHostKeyChecking=no {} '{}'".format(worker_ip, run_cmd)
    shell.execute_command(cmd)
    check_cmd = "docker ps --format {{{{.Names}}}} | grep ^{}$".format(container_name)
    cmd = "ssh -o PasswordAuthentication=no -o StrictHostKeyChecking=no {} '{}'".format(worker_ip, check_cmd)
    out = shell.execute_command(cmd)
    if out.find(container_name) == -1:
        raise RuntimeError("Failed to schedule container from {} to {}".format(admin_ip, worker_ip))
    
    run_cmd = "docker rm -f {}".format(container_name)
    cmd = "ssh -o PasswordAuthentication=no -o StrictHostKeyChecking=no {} '{}'".format(worker_ip, run_cmd)
    shell.execute_command(cmd)

    Logger.info("Succeed to schedule from {} to {}".format(admin_ip, worker_ip))

    
def validate_docker_mode(domain_config):
    validate_user()
    
    global_config = domain_config.global_config
    ipset = set()
    for key in [HapeCommon.SWIFT_KEY, HapeCommon.HAVENASK_KEY, HapeCommon.BS_KEY]:
        admin_iplist = global_config.get_admin_candidate_ips(key)
        worker_iplist = []
        for role, iplist in global_config.get_worker_candidate_maps(key).items():
            worker_iplist.extend(iplist)
        for ip in admin_iplist + worker_iplist:
            if ip in ipset:
                continue
            ipset.add(ip)
            validate_single(key, ip, domain_config)

    ipset = {}
    for key in [HapeCommon.SWIFT_KEY, HapeCommon.HAVENASK_KEY, HapeCommon.BS_KEY]:
        admin_iplist = global_config.get_admin_candidate_ips(key)
        for admin_ip in admin_iplist:
            if admin_ip not in ipset:
                ipset[admin_ip] = set()
            for role, iplist in global_config.get_worker_candidate_maps(key).items():
                for worker_ip in iplist:
                    if worker_ip in ipset[admin_ip]:
                        continue
                    ipset[admin_ip].add(worker_ip)
                    validate_schedule(key, admin_ip, worker_ip, domain_config)
                    
def validate_k8s_service_name(domain_config):
    pattern_str = r'^[a-z0-9]([-a-z0-9]*[a-z0-9])?$'
    pattern = re.compile(pattern_str)
    for key in [HapeCommon.HAVENASK_KEY, HapeCommon.SWIFT_KEY, HapeCommon.BS_KEY]:
        name = domain_config.global_config.get_appmaster_service_name(key)
        if len(name) < 1 or len(name) > 253:
            raise RuntimeError("Length ({}) of service name [{}] is not allowed in k8s"%(len(name), name))

        if not pattern.match(name):
            raise RuntimeError("Service name [{}] is not allowed in k8s, supported pattern: {}".format(name, pattern_str))

                
def validate_k8s_mode(domain_config):
    global_config = domain_config.global_config
    
    binarypath, kubeconfig = global_config.common.binaryPath, global_config.common.kubeConfigPath
    client = K8sClient(binarypath, kubeconfig)
    if not client.check_connections():
        msg = "Failed to connect to k8s cluster in this machine, maybe you should check kube config in [{}]".format(kubeconfig)
        raise RuntimeError(msg)
    
    validate_k8s_service_name(domain_config)
