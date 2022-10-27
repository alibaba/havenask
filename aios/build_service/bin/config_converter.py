#!/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import subprocess
import shutil
import json
import copy

def getAllClusterNames(config_path):
    clusters = []
    clusters_dir = os.path.join(config_path, 'clusters')
    if os.path.exists(clusters_dir):
        clusters = map(lambda f: '_'.join(os.path.basename(f).split('_')[:-1]),
                       filter(lambda x: x.endswith('_cluster.json'), os.listdir(clusters_dir)))
    return clusters

def convert(config_path):
    clusters = getAllClusterNames(config_path)
    processor_chains_config = []
    for cluster in clusters:
        cluster_file = config_path + '/clusters/' + cluster + '_cluster.json'
        print cluster_file
        f = open(cluster_file,  'r')
        cluster_config = json.read(f.read())
        f.close()


        dest_cluster_config = copy.deepcopy(cluster_config)
        dest_cluster_config['swift_config'] = {
            "topic" : {
                "partition_buffer_size" : 1,
                "partition_resource" : 1,
            }
        }

        if 'builder_rule_config' not in dest_cluster_config['cluster_config']:
            dest_cluster_config['cluster_config']['builder_rule_config'] = { }
        if 'build_option_config' not in dest_cluster_config:
            dest_cluster_config['build_option_config'] = { }
        build_option_config = dest_cluster_config['build_option_config']
        builder_rule_config = dest_cluster_config['cluster_config']['builder_rule_config']

        if 'NeedPartition' in build_option_config:
            builder_rule_config['need_partition'] = dest_cluster_config['build_option_config']['NeedPartition']
            del dest_cluster_config['build_option_config']['NeedPartition']

        if 'doc_process_chain_config' in dest_cluster_config:
            del dest_cluster_config['doc_process_chain_config']

        if 'NeedSort' in build_option_config:
            build_option_config['sort_build'] = build_option_config['NeedSort']
            del build_option_config['NeedSort']
        if 'SortFieldName' in build_option_config:
            field_name = build_option_config['SortFieldName']
            del build_option_config['SortFieldName']
            del build_option_config['SortFieldType']
            pattern = 'desc'
            if 'SortPattern' in build_option_config:
                pattern = build_option_config['SortPattern']
                del build_option_config['SortPattern']
            build_option_config['sort_descriptions'] = [
                {
                    "sort_field" : field_name,
                    "sort_pattern" : pattern
                }
            ]
        if 'SortParams' in dest_cluster_config and 'sort_descriptions' not in dest_cluster_config:
            sort_descs = []
            for sort_config in dest_cluster_config['SortParams']:
                sort_descs.append({
                    'sort_field' : sort_config['SortFieldName'],
                    'sort_pattern' : sort_config['SortPattern'] if 'SortPattern' in sort_config else 'desc'
                })
            build_option_config['sort_descriptions'] = sort_descs
            del dest_cluster_config['SortParams']

        f = open(cluster_file, 'w')
        f.write(json.write(dest_cluster_config, format=True))
        f.close()

        table_name = cluster_config['cluster_config']['table_name']
        processor_chain_config = filter(lambda x: x['table_name'] == table_name, processor_chains_config)
        if processor_chain_config:
            processor_chain_config[0]['clusters'].append(cluster)
            continue

        ori_chain = [ { "class_name" : "TokenizeDocumentProcessor" } ]
        ori_sub_chain = [ { "class_name" : "TokenizeDocumentProcessor" } ]
        ori_modules = []
        if 'doc_process_chain_config' in cluster_config:
            if 'document_processor_chain' in cluster_config['doc_process_chain_config']:
                ori_chain = cluster_config['doc_process_chain_config']['document_processor_chain']
            if 'sub_document_processor_chain' in cluster_config['doc_process_chain_config']:
                ori_sub_chain = cluster_config['doc_process_chain_config']['sub_document_processor_chain']
            if 'modules' in cluster_config['doc_process_chain_config']:
                ori_modules = cluster_config['doc_process_chain_config']['modules']

        processor_chain_config = {
            'table_name' : cluster_config['cluster_config']['table_name'],
            'clusters' : [cluster],
            'modules' : ori_modules,
            'document_processor_chain' : ori_chain,
            'sub_document_processor_chain' : ori_sub_chain
        }
        processor_chains_config.append(processor_chain_config)

    if clusters:
        # write processor.json
        f = open(os.path.join(config_path, 'processor.json'), 'w')
        f.write(json.write({ 'processor_chain_config' : processor_chains_config } , format=True))
        f.close()

        # write hippo.json
        # resource = [
        #         {
        #             "amount" : 1,
        #             "name" : "cpu"
        #         },
        #         {
        #             "amount" : 1,
        #             "name" : "mem"
        #         },
        #         {
        #             "amount" : 1,
        #             "name" : "lantao"
        #         }
        #     ]
        # hippo = {
        #     'role_resource' : {
        #         "admin" : resource,
        #         'processor' : resource,
        #     }
        # }
        # for cluster in clusters:
        #     hippo['role_resource']['builder_' + cluster_name] = resource

        # f = open(os.path.join(config_path, 'hippo.json'), 'w')
        # f.write(json.write(hippo , format=True))
        # f.close()

def run(cmd):
    p = subprocess.Popen(cmd, shell=True, close_fds=False,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    return stdout, stderr, p.returncode

def findClusterConfigs(dir):
    data, error ,code = run("find %s -name 'clusters'" % dir)

    return set(data.split("\n"))

if __name__ == '__main__':
    clusters = findClusterConfigs(sys.argv[1])
    map(lambda x:convert(x), map(lambda x : os.path.join(x, '..'), clusters))
