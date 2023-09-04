#!/usr/bin/python2
# *-* coding:utf-8 *-*

import os
import sys
import sys

HERE = os.path.dirname(os.path.realpath(__file__))
sys.path = [HERE] + ["/ha3_install/usr/local/lib/python/site-packages"] + sys.path

import requests
import click

def sql_query(address, query):
    if query.find("kvpair") == -1:
        query += "&&kvpair=databaseName:database;formatType:string"
    
    address = address +  "/QrsService/searchSql"
    try:
        query = query.encode("utf-8")
    except:
        pass
    print("Request address:[{}] query:[{}]".format(address, query))
    data = {
        "assemblyQuery":query
    }
    response = requests.post(address, json=data)
    return response

@click.command()
@click.option("--address", required=False, help="address of qrs (ip:port)", default="http://127.0.0.1:45800")
@click.option("--query", required=True, help="sql query")
def cli(address, query):
    response = sql_query(address, query)
    print(response.text)

if __name__ == "__main__":
    cli()
