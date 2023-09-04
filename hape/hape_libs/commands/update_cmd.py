# -*- coding: utf-8 -*-

from .common import *
from hape_libs.common import *
from hape_libs.container import *
import click

@click.group(short_help='Update some hape config')
def update():
    pass


@update.command(short_help='Update hape template, will restart havenask admin')
@common_params
def template(**kwargs):
    hape_cluster = command_init(kwargs)
    Logger.info("Restart havenask admin to reload template")
    hape_cluster.cluster_config.template_config.upload()
    hape_cluster.restart_havenask_admin()

@update.command(short_help='Update hape candidate iplist')
@common_params
def candidate(**kwargs):    
    hape_cluster = command_init(kwargs)
    hape_cluster.update_candidate_ips()
    
