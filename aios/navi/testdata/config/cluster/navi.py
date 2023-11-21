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
        biz_config = file_loader.load_file(biz_config_file).config(config_path, meta_map, biz_name, conf)
        biz_config["meta_info"] = ""
        biz_config['part_count'] = part_count
        biz_config['part_ids'] = part_ids
        biz_config["modules"] = [
            "../modules/libtest_plugin.so"
        ]
        biz_config_map[biz_name] = biz_config
    return biz_config_map

def config(config_path, meta_info):
    return {
        "version" : 0,
        "log_config" : {
            "file_appenders" : [
                {
                    "file_name" : meta_info["server_name"],
                    "log_level" : "schedule3",
                    "async_flush" : False
                }
            ]
        },
        "engine_config" : {
            "thread_num" : 4,
            "queue_size" : 1000,
            "disable_perf": True,
            "disable_symbol_table": True,
            "builtin_task_queue" : {
                "processing_size" : 3000000
            }
        },
        # "resource_list": ["navi.test.rpc_server_r"],
        "kernel_list": ["TestResourceOverride"],
        "config_path" : config_path,
        "biz_config" : get_biz_config(meta_info, config_path),
        "sleep_before_update_us" : 0,
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
