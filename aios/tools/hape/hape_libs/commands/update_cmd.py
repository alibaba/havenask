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

@update.command(short_help='Update offline table index')
@common_params
@click.option('-t', '--table',  required=True, help="Table name")
@click.option('-p', '--partition', help="Partition count of table", default=1)
@click.option('-s', '--schema',  required=True, help="Schema file path of table")
@click.option('-f', '--full',  required=True, help="Full data path for offline table")
@click.option('-ss', '--swift_start_timestamp',  required=True, help="After full index loaded, havenask will consume this swift realtime message from this timestamp", type=int)
def offline_table_index(**kwargs):
    hape_cluster = command_init(kwargs)
    hape_cluster.update_offline_table(kwargs["table"], kwargs["partition"], kwargs["schema"], kwargs["full"], kwargs["swift_start_timestamp"])
    
@update.command(short_help='Update table schema')
@common_params
@click.option('-t', '--table',  required=True, help="Table name")
@click.option('-p', '--partition', help="Partition count of table", default=1)
@click.option('-s', '--schema',  required=True, help="Schema file path of table")
@click.option('-f', '--full',  required=True, help="Full data path for offline table")
@click.option('-ss', '--swift_start_timestamp',  required=True, help="After full index loaded, havenask will consume this swift realtime message from this timestamp", type=int)
def table_schema(**kwargs):
    hape_cluster = command_init(kwargs)
    hape_cluster.update_offline_table(kwargs["table"], kwargs["partition"], kwargs["schema"], kwargs["full"], kwargs["swift_start_timestamp"])

@update.command(short_help='Update load config')
@common_params
@click.option('-t', '--table',  required=True, help="Table name")
@click.option('-s', '--online_index_config',  required=True, help="Online index config")
def load_strategy(**kwargs):
    hape_cluster = command_init(kwargs)
    hape_cluster.create_or_update_load_strategy(kwargs["table"], kwargs["online_index_config"])

@update.command(short_help='Update hippo config')
@common_params
@click.option('-p', '--hippo_config_path',  required=True, help="hippo config file for role")
@click.option('-r', '--role_type',  required=True, help="role type, [searcher|qrs]")
def hippo_config(**kwargs):
    hape_cluster = command_init(kwargs)
    hape_cluster.update_hippo_config(kwargs["hippo_config_path"], kwargs["role_type"])
