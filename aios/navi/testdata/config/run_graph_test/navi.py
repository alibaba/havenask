import os
import file_loader
import json

def get_biz_config(meta_map, config_path):
    biz_list = meta_map["biz_list"]
    biz_config_map = {}
    for biz_name, conf in biz_list.items():
        biz_config_file = conf['biz_config']
        part_count = conf['part_count']
        part_ids = conf['part_ids']
        biz_config = file_loader.load_file(biz_config_file).config(config_path, meta_map)
        biz_config["meta_info"] = ""
        biz_config['part_count'] = part_count;
        biz_config['part_ids'] = part_ids;
        biz_config_map[biz_name] = biz_config
    return biz_config_map

def config(config_path, meta_info):
    return {
        "version" : 0,
        "log_config" : {
            "file_appenders" : [
                {
                    "file_name" : meta_info["server_name"],
                    "log_level" : "info",
                    "async_flush" : False
                }
            ]
        },
        "engine_config" : {
            "builtin_task_queue" : {
                "thread_num" : 4,
                "queue_size" : 1000,
            },
            "extra_task_queue" : {
                "slow_queue" : {
                    "thread_num" : 4,
                    "processing_size" : 1,
                    "queue_size" : 10,
                },
                "slow_slow_queue" : {
                    "thread_num" : 4,
                    "processing_size" : 1,
                    "queue_size" : 1,
                },
                "slow_slow_slow_queue" : {
                    "thread_num" : 4,
                    "processing_size" : 1,
                    "queue_size" : 0,
                },
                "fast_queue" : {
                    "thread_num" : 4,
                    "processing_size" : 10,
                    "queue_size" : 10,
                },
            },
        },
        "config_path" : config_path,
        "modules" : [
            "../modules/libtest_plugin.so"
        ],
        "biz_config" : get_biz_config(meta_info, config_path),
        "resource_config" : [
            {
                "name" : "navi.buildin.gig_client",
                "config" : {
                    "init_config" : {
                        "subscribe_config" : {
                            "local" : json.loads(meta_info["gig_sub_config"])
                        },
                        "misc_config" : {
                            "snapshot_log_prefix" : meta_info["gig_host"]
                        },
                        "connection_config" : {
                            "grpc_stream" : {
                                "thread_num" : 20
                            }
                        }
                    }
                }
            }
        ]
    }
