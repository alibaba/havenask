from .logger import Logger
import json
import os
import traceback
from ConfigParser import ConfigParser
from .logger import Logger

class UnsupportedFileTypeError(Exception):
    pass

def _read_json(abs_path):
    with open(abs_path, "r") as f:
        return json.load(f)

def _read_conf(abs_path):
    config = ConfigParser()
    config.read(abs_path)
    return {s:dict(config.items(s)) for s in config.sections()}

def _write_json(abs_path, dict_obj):
    with open(abs_path, "w") as f:
        return json.dump(dict_obj, f, indent=4)

def _write_conf(abs_path, dict_obj):
    config = ConfigParser()
    for key in dict_obj:
        config.add_section(key)
        for subkey in dict_obj[key]:
            # print("set {} as {}".format(key, dict_obj[key][subkey]))
            config.set(key, subkey, dict_obj[key][subkey])
    config.write(open(abs_path, 'w'))
    

class DictFileUtil:
    support_types = set(["json", "conf"])
    
    read_funcs = {
        "json": _read_json,
        "conf": _read_conf
    }
    
    write_funcs = {
        "json": _write_json,
        "conf": _write_conf
    }
    
    @staticmethod
    def parse_type(path, type):
        if type != None:
            return type
        
        if path.find(".") != -1:
            suffix = path.split(".")[-1] 
            if suffix in DictFileUtil.support_types:
                return suffix
            raise UnsupportedFileTypeError("unsupport file type: {}".format(suffix))
        raise UnsupportedFileTypeError("unsupport file type: None")
            
    @staticmethod
    def read(abs_path, type=None, supress_warning=False):
        if not os.path.exists(abs_path):
            message = "file not found: {}".format(abs_path)
            if not supress_warning:
                Logger.warning(message)
            raise IOError(message)
        
        type = DictFileUtil.parse_type(abs_path, type)
        return DictFileUtil.read_funcs[type](abs_path)
    
    @staticmethod
    def write(abs_path, dict_obj, type=None):
        type = DictFileUtil.parse_type(abs_path, type)
        return DictFileUtil.write_funcs[type](abs_path, dict_obj)
           