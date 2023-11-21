# -*- coding: utf-8 -*-

from .common import *
from hape_libs.common import *
from hape_libs.container import *
import click

@click.group(short_help='get hape config')
def get():
    pass

@get.command(short_help='Get default hippo config by role type')
@common_params
@click.option('-r', '--role_type',  required=True, help="role type[searcher|qrs]")
def default_hippo_config(**kwargs):
    hape_cluster = command_init(kwargs, logging_level=logging.ERROR)
    hape_cluster.get_default_hippo_config(kwargs["role_type"])
