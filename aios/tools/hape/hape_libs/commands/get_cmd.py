# -*- coding: utf-8 -*-

from .common import *
import click
import json

@click.group(short_help='get hape config')
def get():
    pass

@get.command(short_help='Get default hippo config by role type')
@common_params
@click.option('-r', '--role_type',  required=True, help="role type[searcher|qrs]")
def default_hippo_config(**kwargs):
    havenask_domain = command_init(kwargs, logging_level=logging.ERROR)
    print(json.dumps(havenask_domain.get_default_hippo_config(kwargs["role_type"]), indent=4))
