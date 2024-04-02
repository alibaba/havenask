# -*- coding: utf-8 -*-

from .common import *
from hape_libs.common import HapeCommon
import click

@click.group(short_help='Restart cluster, will recover service from zookeeper')
def restart():
    pass
@restart.command()
@common_params
def swift(**kwargs):
    havenask_domain = command_init(kwargs)
    havenask_domain.start(HapeCommon.SWIFT_KEY, allow_restart=True)
    
@restart.command()
@common_params
def havenask(**kwargs):
    havenask_domain = command_init(kwargs)
    havenask_domain.start(HapeCommon.HAVENASK_KEY, allow_restart=True)
