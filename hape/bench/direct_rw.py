import requests
import click
import json
import random
import time

column_names = ['id', 'hits', 'title']

@click.command()
@click.option("--num", default=10000)
@click.option("--batch", default=100)
@click.option("--start", default=0)
@click.option("--address", required=True)
@click.option("--table", required=True)
@click.option("--strlen", default=1000)
def direct_rw(num, batch, start, address, table, strlen):
    fields_str = ','.join(column_names)
    values_str = ','.join(['?' for i in range(len(column_names))])
    timestamp = time.time()
    for i in range(start , start + num):
        id = str(i)
        hits = str(i)
        title = "\""+" ".join([str(x) for x in range(strlen)])+"\""
        data = [id, hits, title]
        param = ','.join(data)
        param = "[" + param + "]"
        query = '''insert into {} ({}) values({})&&kvpair=formatType:json;timeout:10000;searchInfo:true;exec.leader.prefer.level:1;iquan.plan.cache.enable:true;\
databaseName:database;dynamic_params:[{}]'''.format(table, fields_str,values_str,param)
        request = {'assemblyQuery': query}
        succ = False
        while not succ:
            response = requests.post("http://"+address+"/QrsService/searchSql",json=request)
            response = response.json()
            succ = (json.dumps(response).find("ERROR_NONE")!=-1)
            if not succ:
                print("write failed, detail:")
                print(json.dumps(response))
        # print(response.json())
        if i % batch == 0 and i!=0:
            print("use time {} for {} msgs, {}/{}".format(time.time() - timestamp, batch, i, num))
            time.sleep(1)
            query = "select max(id) from {} && kvpair=formatType:json;timeout:10000;databaseName:database".format(table)
            request = {'assemblyQuery': query}
            response = requests.post("http://"+address+"/QrsService/searchSql",json=request)
            print(response.json())
            timestamp = time.time()
        
    
    
if __name__ == "__main__":
    direct_rw()
    
