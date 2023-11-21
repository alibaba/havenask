#!/usr/bin/python2
# *-* coding:utf-8 *-*

import os
import sys
import tempfile

HERE = os.path.dirname(os.path.realpath(__file__))
sys.path = [HERE] + ["/ha3_install/usr/local/lib/python/site-packages"] + sys.path

import click
from hape_libs.utils.havenask_dataset import HavenaskDataSet
from hape_libs.utils.shell import LocalShell
from hape_libs.utils.pk_hash import getPartitionIdByStr

SWIFT_TOOL_PATH = "/ha3_install/usr/local/bin/swift"

def write_swift_message_bydict(zk, topic, count, shard_field, data_dict):
    doc = ""
    for key, value in data_dict.items():
        doc += key + "=" + str(value) + HavenaskDataSet.field_sep+ "\n"
    doc += HavenaskDataSet.doc_sep + "\n"
    
    with tempfile.NamedTemporaryFile() as tmp:
        f = open(tmp.name, "w")
        pk = data_dict[shard_field]
        part = getPartitionIdByStr(str(pk), count)
        f.write(doc)
        f.close()
        cmd = SWIFT_TOOL_PATH + " sm -z {} -t {} -p {} -m {} -s \"\x1e\"".format(zk, topic, part, tmp.name)
        print("write data cmd: {}".format(cmd))
        out, succ = LocalShell.execute_command(cmd, grep_text="success")
        if succ:
            print("Succeed to write data")
        else:
            print("Failed to write data, detail:{}".format(out))
    

def write_swift_message_byfile(zk, topic, count, schema, file):
    dataset = HavenaskDataSet(file, schema)
    dataset.parse()
    schema = dataset.schema
    shard_field = schema.shard_field
    for record in dataset.records:
        pk = record.get(shard_field)
        part = getPartitionIdByStr(str(pk), count)
        with tempfile.NamedTemporaryFile() as tmp:
            f = open(tmp.name, "w")
            doc = ""
            for key, value in record.raw_doc_dict.items():
                doc += key + "=" + value + HavenaskDataSet.field_sep + "\n"
            doc += HavenaskDataSet.doc_sep + "\n"
                
            f.write(doc)
            f.close()
            print("save data in tmp file, path: {}, content: {}".format(tmp.name, doc))
            cmd = SWIFT_TOOL_PATH + " sm -z {} -t {} -p {} -m {} -s \"\x1e\"".format(zk, topic, part, tmp.name)
            print("write swift data cmd: {}".format(cmd))
            out, succ = LocalShell.execute_command(cmd, grep_text="success")
            if succ:
                print("Succeed to write swift data")
            else:
                print("Failed to write swift data, detail:{}".format(out))
    

@click.command()
@click.option("--zk", required=True, help="service zk address of swift")
@click.option("--topic", required=True, help="swift topic name")
@click.option("--count", required=True, help="swift topic partition count" , type=int)
@click.option("--schema", required=True, help="schema file")
@click.option("--file", required=True, help="swift data file")
def cli(zk, topic, count, schema, file):
    write_swift_message_byfile(zk, topic, count, schema, file)


if __name__ == "__main__":
    cli()
