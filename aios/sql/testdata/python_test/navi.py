import os
import file_loader
import json

def config(config_path, meta_info):
    biz_config_map = {
        "init_resource" : [
            "iquan_resource"
        ],
        "resource_config" : [
            {
                "name" : "iquan_resource",
                "config": {
                    "key1" : 3
                }
            }
        ]
    }
    return biz_config_map
