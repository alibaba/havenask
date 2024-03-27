# -*- coding: utf-8 -*-

import click
import logging

from .common import *
from hape_libs.common import HapeCommon

@click.group()
def gs():
    '''
    Get status of havenask cluster
    '''
    pass
    
@gs.command()
@common_params
def havenask(**kwargs):
    havenask_domain = command_init(kwargs, logging_level=logging.ERROR)
    havenask_domain.get_cluster_status(HapeCommon.HAVENASK_KEY)
    
@gs.command()
@common_params
@click.option("-t", "--table", help="specify table name")
@click.option("-d", "--detail", help="Detail havenask cluster status", default=False, is_flag=1, required=False)
def table(**kwargs):
    havenask_domain = command_init(kwargs, logging_level=logging.ERROR)
    havenask_domain.get_table_status(kwargs['table'], kwargs["detail"])
    
@gs.command()
@common_params
def swift(**kwargs):
    havenask_domain = command_init(kwargs, logging_level=logging.ERROR)
    havenask_domain.get_cluster_status(HapeCommon.SWIFT_KEY)
    

@gs.command()
@common_params
@click.option("-t", "--table", help="specify table name")
def bs(**kwargs):
    havenask_domain = command_init(kwargs, logging_level=logging.ERROR)
    havenask_domain.get_cluster_status(HapeCommon.BS_KEY, table=kwargs["table"])