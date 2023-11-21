# -*- coding: utf-8 -*-

from .common import *
from hape_libs.common import *
import click

@click.group(short_help='Stop cluster. Will only stop containers and not clean zookeeper and database files ')
def stop():
    pass

@stop.command()
@common_params
def swift(**kwargs):
    hape_cluster = command_init(kwargs)
    hape_cluster.stop_hippo_appmaster(HapeCommon.SWIFT_KEY)

@stop.command()
@common_params
def havenask(**kwargs):
    hape_cluster = command_init(kwargs)
    hape_cluster.stop_hippo_appmaster(HapeCommon.HAVENASK_KEY)


@stop.command()
@common_params
@click.option("-i", "--ip", help="Ip of container", required=True)
@click.option("-n", "--name", help="Name of container", required=True)
def container(**kwargs):
    hape_cluster = command_init(kwargs)
    hape_cluster.stop_container(kwargs["ip"], kwargs["name"])

@stop.command()
@common_params
@click.option("-t", "--table_name", help="table name", required=True)
@click.option("-g", "--generation_id", help="generation id", required=True)
def build(**kwargs):
    hape_cluster = command_init(kwargs)
    hape_cluster.drop_build(kwargs["table_name"], kwargs["generation_id"])
