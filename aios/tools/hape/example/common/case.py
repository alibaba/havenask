#! /usr/bin/python
# *-* coding:utf-8 *-*

import os
import sys
import subprocess
import time

HERE = os.path.dirname(os.path.realpath(__file__))
CASE_DIR = os.path.realpath(os.path.join(HERE, "../../example/cases"))
import sys
sys.path = [HERE] + ["/ha3_install/usr/local/lib/python/site-packages"] + sys.path
import click
from hape_libs.utils.havenask_dataset import HavenaskDataSet
from hape_libs.utils.sql_query import sql_query
from hape_libs.utils.logger import Logger
from hape_libs.utils.retry import Retry
import logging
Logger.init_stdout_logger(logging.INFO)
from hape_libs.utils.swift_writer import write_swift_message_byfile


case_config = None

def run_command(cmd, with_config=True):
    if with_config:
        cmd += " -c {}".format(case_config.hape_conf)
    Logger.info("Execute cmd : {}".format(cmd))
    p = subprocess.Popen(cmd, shell=True, stdout=sys.stdout, stderr=sys.stderr)
    p.communicate()

def run_command_wth_return(cmd):
    cmd += " -c {}".format(case_config.hape_conf)
    Logger.info("Execute cmd : {}".format(cmd))
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, error = p.communicate()
    return out + "/n" + error
    

def load_case_config(case):
    case_path = os.path.join(CASE_DIR, case)
    sys.path = [case_path] + sys.path
    print("sys path {}".format(sys.path))
    try:
        from setup import CaseConfig
    except Exception as e:
        Logger.error("Case {} not found".format(case))
        Logger.error(str(e))
        return False
    
    global case_config
    case_config = CaseConfig()
    return True

def check_havenask_ready():
    pass


@click.group()
def cli():
    pass

    
@cli.command()
@click.option("--case", help="Case name", required=True)
@click.option("--type", help="Type of table", default="direct", type=click.Choice(['direct', 'offline']))
def run(case, type):
    if not load_case_config(case):
        return False
    
    run_command("hape delete havenask")
    run_command("hape delete swift")
    run_command("rm -rf ~/*havenask-sql-local*", with_config=False)
    run_command("rm -rf ~/*havenask-swift-local*", with_config=False)
    run_command("rm -rf ~/*havenask-bs-local*", with_config=False)
    run_command("hape start havenask")
    
    table = "in0"
    if type == "direct":
        run_command("hape create table -t {} -s {}".format(table, case_config.schema))
        
        def check_havenask_ready():
            out = run_command_wth_return("hape gs havenask")
            Logger.info("Current havenask status: {}".format(out))
            if out.find("NOT_READY") == -1:
                return True
            else:
                return False
        msg = "havenask cluster ready"
        succ = Retry.retry(check_havenask_ready, msg, limit=20)
        
        Logger.info("Start to insert realtime data")
        full_dataset = HavenaskDataSet(case_config.full_data, case_config.schema)
        full_dataset.parse()
        insert_sqls = full_dataset.to_sqls(table)
        if case_config.rt_data != None:
            rt_dataset = HavenaskDataSet(case_config.rt_data, case_config.schema)
            rt_dataset.parse()
            insert_sqls.extend(rt_dataset.to_sqls(table))
        
        
        for idx, sql in enumerate(insert_sqls):
            Logger.info("insert sql {}/{}".format(idx, len(insert_sqls)))
            def check_result():
                result = sql_query("http://localhost:45800", sql + ";formatType:json;timeout:20000")
                Logger.info("Response: {}".format(result.text))
                try:
                    data = result.json()
                    return data["error_info"].find('ERROR_NONE') != -1
                except:
                    return False
            succ = Retry.retry(check_result, "insert sql succeed", 10)
            if not succ:
                Logger.error("Failed to insert data")

    else:
        Logger.info("Start to build ful data")
        run_command("hape create table -t {} -s {} -f {}".format(table, case_config.schema, case_config.full_data))
        def check_havenask_ready():
            out = run_command_wth_return("hape gs bs")
            Logger.info("Offline table need to start multiple buildservice processors, it will take longer times")
            Logger.info("Current build status: ")
            Logger.info(out)
            out = run_command_wth_return("hape gs havenask")
            if out.find("NOT_READY") == -1:
                return True
            else:
                return False
        msg = "full data build finished and ready in havenask cluster"
        succ = Retry.retry(check_havenask_ready, msg, limit=120)

        Logger.info("Start to push realtime data")
        if not succ:
            return 
        if case_config.rt_data != None:
            write_swift_message_byfile("zfs://127.0.0.1:2181/havenask/havenask-swift-local", table, 1, case_config.schema, case_config.rt_data)
    
    time.sleep(5)
    Logger.info("Start to query data")
    for sql in case_config.queries:
        result = sql_query("http://localhost:45800", sql)
        print(result.text)
        
@cli.command()
@click.option("--case", help="Case name", required=True)
@click.option("--command", help="Hape command", required=True)
def execute(case, command):
    if not load_case_config(case):
        return False
    run_command(command)

    
if __name__ == "__main__":
    cli()

