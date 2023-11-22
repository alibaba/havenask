# -*- coding: utf-8 -*-

from .common import *
import traceback
import click
import getpass

from hape_libs.common import *
from hape_libs.utils.logger import Logger
from hape_libs.utils.fs_wrapper import FsWrapper
from hape_libs.utils.shell import SSHShell
from hape_libs.container import *


@click.command(short_help='Validate hape env and domain conf')
@common_params
def validate(**kwargs):
    try:
        hape_cluster = command_init(kwargs)
    except Exception as e:
        Logger.error(traceback.format_exc())
        raise RuntimeError("Failed to init hape config, maybe has wrong domain config argument or failed to access zfs/hdfs/local path")
    
    validate_user()
    
    cluster_config = hape_cluster.cluster_config
    ipset = set()
    for key in [HapeCommon.SWIFT_KEY, HapeCommon.HAVENASK_KEY, HapeCommon.BS_KEY]:
        admin_iplist = cluster_config.get_admin_ip_list(key)
        worker_iplist = cluster_config.get_worker_ip_list(key)
        for ip in admin_iplist + worker_iplist:
            if ip in ipset:
                continue
            ipset.add(ip)
            validate_single(key, ip, cluster_config)

    ipset = {}
    for key in [HapeCommon.SWIFT_KEY, HapeCommon.HAVENASK_KEY, HapeCommon.BS_KEY]:
        admin_iplist = cluster_config.get_admin_ip_list(key)
        worker_iplist = cluster_config.get_worker_ip_list(key)
        for admin_ip in admin_iplist:
            if admin_ip not in ipset:
                ipset[admin_ip] = set()
            for worker_ip in worker_iplist:
                if worker_ip in ipset[admin_ip]:
                    continue
                ipset[admin_ip].add(worker_ip)
                validate_schedule(key, admin_ip, worker_ip, cluster_config)
                
    # validate_port(cluster_config)
    Logger.info("Succeed to validate basic hape enviroment")

def validate_user():
    user = getpass.getuser()
    not_supported_users = ['root']
    if user in not_supported_users:
        raise RuntimeError("account:{} is not supported, please use anothor acount".format(user))
    
    home = os.path.expanduser("~")
    if not home.startswith("/home"):
        raise RuntimeError("please make sure account:{} has home directory".format(user))
    
## check every machine 
def validate_single(key, ip, cluster_config):
    Logger.info("Begin to validate single container")
    
    ## check if image exists and container can be created
    image = cluster_config.get_domain_config_param(key, "image")
    container_service= RemoteDockerContainerService(cluster_config)
    container_name = "havenask-validate-single"
    container_meta = ContainerMeta(container_name, ip)
    container_spec = ContainerSpec(image, 100, 5124)
    
    shell = SSHShell(ip)
    out, succ = shell.execute_command("docker images | grep {}".format(".*".join(image.split(":"))), grep_text=image.split(":")[0])
    if not succ:
        raise RuntimeError("Image {} not found in ip {}".format(image, ip))
    
    succ = container_service.create_container(container_meta, container_spec)
    if not succ:
        raise RuntimeError("Failed to create container")
    
    container_service.stop_container(container_meta, container_spec)

    # check path in global.conf is accessable
    # binary = cluster_config.get_domain_config_param(HapeCommon.COMMON_CONF_KEY, "binaryPath")
    # out, fail = container_service.execute_command(container_meta, ProcessorSpec({}, {}, "ls {}".format(binary)), grep_text="No such")
    # if fail:
    #     raise RuntimeError("No binary path:[{}] in container [{}], ip:[{}]".format(binary, container_name, ip))    
    
    # dataStoreRoot = cluster_config.get_domain_config_param(HapeCommon.COMMON_CONF_KEY, "dataStoreRoot")
    # container_service.execute_command(container_meta, ProcessorSpec({}, {}, "{}/usr/local/bin/fs_util mkdir -p {}".format(binary, dataStoreRoot)))
    # check_paths = [
    #     cluster_config.get_domain_config_param(HapeCommon.COMMON_CONF_KEY, "domainZkRoot"),
    #     cluster_config.get_domain_config_param(HapeCommon.COMMON_CONF_KEY, "binaryPath"),
    #     dataStoreRoot,
    #     cluster_config.get_domain_config_param(HapeCommon.SWIFT_KEY, "swiftZkRoot")
    # ]
    # for check_path in check_paths:
    #     container_service.execute_command(container_meta, ProcessorSpec({}, {}, "{} {}/usr/local/bin/fs_util mkdir -p {}".format(binary, check_path)))
    #     out = container_service.execute_command(container_meta, ProcessorSpec({}, {}, "{} {}/usr/local/bin/fs_util ls {}".format(binary, check_path)))
    #     if out.find("parse file protocol fail!") != -1 or out.find("path does not exist") != -1 or out.find("unknown error!") != -1: 
    #         raise RuntimeError("Failed to create or access path:[{}] in container [{}], ip:[{}]".format(check_path, container_name, ip))

    Logger.info("Succeed to validate single container")
    
    
## check each admin can schedule workers
def validate_schedule(key, admin_ip, worker_ip, cluster_config):
    Logger.info("Begin to schedule from {} to {}".format(admin_ip, worker_ip))
    container_name = "havenask-validate-schedule"
    shell = SSHShell(admin_ip)
    user_home = cluster_config.get_default_var("userHome")
    image = cluster_config.get_domain_config_param(key, "image")
    run_cmd = "docker run --workdir {} --volume=\"/etc/group:/etc/group:ro\" --volume=\"/etc/passwd:/etc/passwd:ro\"  --volume=\"/etc/shadow:/etc/shadow:ro\" -v {}:{} --ulimit nofile=655350:655350  \
--ulimit memlock=-1 --ulimit core=-1 --network=host --privileged -d  --cpu-quota=500000 \
--cpu-period=100000 --memory=5124m -v /etc/passwd:/home/.passwd -v /etc/group:/home/.group --name {} {} /sbin/init".format(user_home, user_home, user_home, container_name, image)
    cmd = "ssh -o PasswordAuthentication=no -o StrictHostKeyChecking=no {} '{}'".format(worker_ip, run_cmd)
    shell.execute_command(cmd)
    check_cmd = "docker ps | grep {}".format(container_name)
    cmd = "ssh -o PasswordAuthentication=no -o StrictHostKeyChecking=no {} '{}'".format(worker_ip, check_cmd)
    out = shell.execute_command(cmd)
    if out.find(container_name) == -1:
        raise RuntimeError("Failed to schedule container from {} to {}".format(admin_ip, worker_ip))
    
    run_cmd = "docker rm -f {}".format(container_name)
    shell.execute_command(run_cmd)

    Logger.info("Succeed to schedule from {} to {}".format(admin_ip, worker_ip))


## check port is not occupied
# def validate_port(cluster_config):
#     Logger.info("Begin to validate port")
#     qrs_iplist = cluster_config.get_domain_config_param(HapeCommon.HAVENASK_KEY, "qrsIpList").split(";")
#     searcher_iplist =  cluster_config.get_domain_config_param(HapeCommon.HAVENASK_KEY, "searcherIpList").split(";")
#     admin_iplist = cluster_config.get_domain_config_param(HapeCommon.HAVENASK_KEY, "adminIpList").split(";")
#     admin_port = cluster_config.get_domain_config_param(HapeCommon.HAVENASK_KEY, "adminHttpPort").split(";")
#     checks = []
#     checks += [(ip, "45800") for ip in qrs_iplist]
#     checks += [(ip, "45900") for ip in searcher_iplist]
#     checks += [(ip, admin_port) for ip in admin_iplist]
#     for ip, port in checks:
#         shell = SSHShell(ip)
#         cmd = "netstat -nlp | grep {}".format(port)
#         out, flag = shell.execute_command(cmd, grep_text="LISTEN")
#         if flag:
#             raise RuntimeError("Port {} is occupied in ip:{}. This error can be ignored if it belongs to already existing havenask cluster.\
# Otherwise you need to free it".format(port, ip))
#     Logger.info("Succeed to validate port")
    
    