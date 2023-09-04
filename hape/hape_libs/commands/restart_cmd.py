# -*- coding: utf-8 -*-

from .common import *
from hape_libs.common import *
import click

@click.group(short_help='Restart cluster, will recover service from zookeeper')
def restart():
    pass
@restart.command()
@common_params
def swift(**kwargs):
    hape_cluster = command_init(kwargs)
    hape_cluster.start_hippo_appmaster(HapeCommon.SWIFT_KEY, allow_restart=True)
    
@restart.command()
@common_params
def havenask(**kwargs):
    hape_cluster = command_init(kwargs)
    hape_cluster.start_hippo_appmaster(HapeCommon.HAVENASK_KEY, allow_restart=True)
    
    
@restart.command()
@common_params
@click.option("-i", "--ip", help="Ip of container", required=True)
@click.option("-n", "--name", help="Name of container", required=True)
def container(**kwargs):
    hape_cluster = command_init(kwargs)
    hape_cluster.restart_container(kwargs["ip"], kwargs["name"])
    
