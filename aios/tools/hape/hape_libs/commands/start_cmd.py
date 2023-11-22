# -*- coding: utf-8 -*-

from .common import *
from hape_libs.common import *
import click

@click.group(short_help='Start cluster')
def start():
    pass

@click.group(short_help='Create table in havenask cluster')
def create():
    pass

@start.command()
@common_params
def swift(**kwargs):
    hape_cluster = command_init(kwargs)
    hape_cluster.start_hippo_appmaster(HapeCommon.SWIFT_KEY)
    
@start.command()
@common_params
def havenask(**kwargs):
    hape_cluster = command_init(kwargs)
    hape_cluster.start_hippo_appmaster(HapeCommon.HAVENASK_KEY)
    
@create.command()
@common_params
@click.option('-t', '--table',  required=True, help="Table name")
@click.option('-p', '--partition', help="Partition count of table", default=1)
@click.option('-s', '--schema',  required=True, help="Schema file path of table")
@click.option('-f', '--full',  required=False, help="Full data path for offline table (not needed by direct table)")
def table(**kwargs):
    hape_cluster = command_init(kwargs)
    hape_cluster.create_table(kwargs["table"], kwargs["partition"], kwargs["schema"], kwargs["full"])

