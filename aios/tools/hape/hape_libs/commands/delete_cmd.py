# -*- coding: utf-8 -*-

from .common import *
from hape_libs.common import *
import click

@click.group(short_help='Delete cluster, will stop processors, and then clean zk & database runtime files')
def delete():
    pass
@delete.command()
@common_params
def swift(**kwargs):
    hape_cluster = command_init(kwargs)
    hape_cluster.stop_hippo_appmaster(HapeCommon.SWIFT_KEY, is_delete=True)
    
@delete.command()
@common_params
def havenask(**kwargs):
    hape_cluster = command_init(kwargs)
    hape_cluster.stop_hippo_appmaster(HapeCommon.HAVENASK_KEY, is_delete=True)
    
@delete.command()
@common_params
def all(**kwargs):
    hape_cluster = command_init(kwargs)
    hape_cluster.stop_hippo_appmaster(HapeCommon.HAVENASK_KEY, is_delete=True)
    hape_cluster.stop_hippo_appmaster(HapeCommon.SWIFT_KEY, is_delete=True)
    
@delete.command()
@click.option('-t', '--table',  required=True, help="Table name")
@click.option('--keeptopic',  is_flag=True, help="Print more output.", default=False)
@common_params
def table(**kwargs):
    hape_cluster = command_init(kwargs)
    hape_cluster.delete_table(kwargs["table"], kwargs["keeptopic"])
    
