# -*- coding: utf-8 -*-

import click
import logging

from .common import *
from hape_libs.common import *

@click.group()
def gs():
    '''
    Get status of havenask cluster
    '''
    pass
    
@gs.command()
@common_params
@click.option("-d", "--detail", help="Detail havenask cluster status", default=False, is_flag=1, required=False)
def havenask(**kwargs):
    hape_cluster = command_init(kwargs, logging_level=logging.ERROR)
    hape_cluster.get_sql_cluster_status(kwargs["detail"])
    
@gs.command()
@common_params
@click.option("-t", "--table", help="specify table name")
def table(**kwargs):
    hape_cluster = command_init(kwargs, logging_level=logging.ERROR)
    hape_cluster.get_table_status(kwargs['table'])
    
@gs.command()
@common_params
def swift(**kwargs):
    hape_cluster = command_init(kwargs, logging_level=logging.ERROR)
    hape_cluster.get_swift_cluster_status()
    

@gs.command()
@common_params
def bs(**kwargs):
    hape_cluster = command_init(kwargs, logging_level=logging.ERROR)
    hape_cluster.get_bs_cluster_status()