import good_schema
import json
import os

def config(config_path, meta_info):
    bad_config = open(os.path.join(config_path, "bad.json"), "r").read()
    return {
        "schemas" : {
            "good" : good_schema.generate(),
            "bad" : json.loads(bad_config)
        },
        "qrs" : {
        },
        "bizs" : {
        }
    }
