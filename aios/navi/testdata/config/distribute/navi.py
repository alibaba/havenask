import os
import file_loader
import json

def get_gig_config(meta_map):
    server_id = meta_map['server_id']
    if server_id >= 0:
        return {}
    port_array = meta_map['server_port']
    local_sub_config = []
    i = 0
    part_count = len(port_array)
    for port in port_array:
        node = {
            "biz_name" : "server_biz",
            "part_count" : part_count,
            "part_id" : i,
            "version" : 0,
            "weight" : 100,
            "ip" : "0.0.0.0",
            "grpc_port" : port
        }
        local_sub_config.append(node)
        i = i + 1

    return {
        "subscribe_config" : {
            "local" : local_sub_config
        },
        "connection_config" : {
            "grpc_stream" : {
                "thread_num" : 20
            }
        }
    }

def get_biz_list(meta_map):
    server_id = meta_map['server_id']
    if server_id < 0:
        return ['client_biz0']
    else:
        return ['server_biz']

def get_log_file(meta_map):
    server_id = meta_map['server_id']
    if server_id >= 0:
        return "server_%d.log" % (server_id)
    else:
        return "client.log"

def config(config_path, meta_info):
    meta_map = json.loads(meta_info)
    biz_list = get_biz_list(meta_map)
    biz_config_map = {}
    for biz in biz_list:
        biz_config_map[biz] = {
            "config_path" : config_path,
            "meta_info" : meta_info,
            "modules" : [
                "../modules/libtest_plugin.so"
            ],
            "resource_config" : {
            }
        }

    resource_config = {}
    gig_init_config = get_gig_config(meta_map)
    if gig_init_config:
        resource_config = {
            "navi.buildin.gig_client" : {
                "init_config" : gig_init_config
            }
        }

    return {
        "version" : 0,
        "log_config" : {
            "file_appenders" : [
                {
                    "file_name" : get_log_file(meta_map),
                    "log_level" : "error",
                    "async_flush" : False
                }
            ]
        },
        "engine_config" : {
            "thread_num" : 20,
            "queue_size" : 1000
        },
        "biz_config" : biz_config_map,
        "resource_config" : resource_config
    }
