# -*- coding: utf-8 -*-

from .common import *
from hape_libs.clusters import *
from hape_libs.common import HapeCommon
import click

@click.group(short_help='Delete cluster, will stop processors, and then clean zk & database runtime files')
def delete():
    pass

@delete.command()
@common_params
def swift(**kwargs):
    havenask_domain = command_init(kwargs)
    havenask_domain.stop(HapeCommon.SWIFT_KEY, is_delete=True)
    
@delete.command()
@common_params
def havenask(**kwargs):
    havenask_domain = command_init(kwargs)
    havenask_domain.stop(HapeCommon.HAVENASK_KEY, is_delete=True)
    
@delete.command()
@common_params
def all(**kwargs):
    havenask_domain = command_init(kwargs)
    havenask_domain.stop(HapeCommon.HAVENASK_KEY, is_delete=True)
    havenask_domain.stop(HapeCommon.SWIFT_KEY, is_delete=True)
    
@delete.command()
@click.option('-t', '--table',  required=True, help="Table name")
@click.option('--keeptopic',  is_flag=True, help="Print more output.", default=False)
@common_params
def table(**kwargs):
    havenask_domain = command_init(kwargs)
    havenask_domain.delete_table(kwargs["table"], kwargs["keeptopic"])
    
