# -*- coding: utf-8 -*-

import os
HERE = os.path.split(os.path.realpath(__file__))[0]
import sys
sys.path.append(os.path.abspath(os.path.join(HERE, "..","../","hape_depends")))
from kafka import KafkaProducer
from pk_hash import hashString, getPartitionId
from argparse import ArgumentParser

def parse_doc(doc, pk_field):
    for line in doc.split("\n"):
        if line.startswith(pk_field+"="):
            return (line.split("=")[-1]).strip()[:-1]

def send_rt_doc(rt_address, rt_topicname, rt_partcnt, doc_path, pk_field):
    producer = KafkaProducer(bootstrap_servers=rt_address)
    with open(doc_path, "r") as f:
        doc = f.read()
    pk_value = parse_doc(doc, pk_field)
    hashed_pk = hashString(pk_value)
    partition_id = getPartitionId(hashed_pk, rt_partcnt)
    print("topic name: {}".format(rt_topicname))
    print("hashed pk: {}".format(hashed_pk))
    print("doc:{}".format(doc))
    print("kafka partition id {}".format(partition_id))
    respponse = producer.send(rt_topicname, key=pk_value, value=doc, partition = partition_id)
    print(respponse.get(timeout=10))


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--rt_address")
    parser.add_argument("--rt_topicname")
    parser.add_argument("--rt_partcnt")
    parser.add_argument("--doc_path")
    parser.add_argument("--pk_field")
    args = parser.parse_args()
    send_rt_doc(args.rt_address, args.rt_topicname, int(args.rt_partcnt), args.doc_path, args.pk_field)