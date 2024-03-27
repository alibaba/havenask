# -*- coding: utf-8 -*-

from .common import *
from hape_libs.utils.logger import Logger
import logging
import click
import subprocess
import sys

@click.command()
@click.option("--ip", required=True, help="List of ip, seperated by ','")
@click.option("--source", required=True, help="Source File or directory to be deploy")
@click.option("--target", required=True, help="Target path of file or directory in remote servers")
def dp(ip, source, target):
    '''
    Deploy files to multiple servers
    '''
    Logger.init_stdout_logger(logging.INFO)
    iplist = ip.split(",")
    ip_to_procs = {}
    for ip in iplist:
        command = "rsync -r -v {} {}:{}".format(source, ip, target)
        proc = subprocess.Popen(command, shell=True, stdout=sys.stdout, stderr=sys.stderr)
        ip_to_procs[ip] = proc
        
    for ip in iplist:
        proc = ip_to_procs[ip]
        Logger.info("Wait ip:{} finish".format(ip))
        proc.communicate()
    
    
