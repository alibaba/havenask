import os

def config(config_path, meta_info):
    return {
        "log_config" : {
            "file_appenders" : [
                {
                    "file_name" : "navi.log",
                    "log_level" : "DEBUG"
                }
            ]
        },
        "engine_config" : {
            "thread_num" : 10,
            "queue_size" : 1000
        },
        "biz_config" : {
            "biz" : {
                "config_path" : config_path,
                "meta_info" : meta_info,
                "modules" : [
                    os.path.join(config_path, "modules/libtest_plugin.so"),
                    "modules/libtest_plugin.so"
                ],
                "resource_config" : {
                    "index_partition" : {
                        "version" : 0,
                        "config" : {
                            "sub" : True,
                            "index_type" : "type"
                        }
                    },
                    "index_partition_v2" : {
                        "version" : 2,
                        "config" : {
                            "sub" : {
                                "kkv_table" : True,
                                "index_table" : False
                            },
                            "index_type" : "type"
                        }
                    }
                }
            }
        }
    }
