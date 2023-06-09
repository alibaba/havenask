import sys
import json
import entry_loader

def load(config_path, loader_param):
    param_map = json.loads(loader_param)
    meta_info = None
    if "meta_info" in param_map:
        meta_info = param_map["meta_info"]
    return entry_loader.load(config_path, meta_info)
