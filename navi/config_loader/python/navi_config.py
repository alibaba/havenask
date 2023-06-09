import json
import sys
import os
import traceback
import file_loader
import logging

def navi_config(loader, config_path, loader_param):
    formatStr = '[%(asctime)s] [%(process)s] [%(filename)s:%(lineno)d] [%(levelname)s] - [%(message)s]'
    datefmtStr = '%m-%d %H:%M:%S'
    logging.basicConfig(level=logging.INFO, format = formatStr, datefmt = datefmtStr)
    logging.info("loader: " + loader)
    try:
        config = None
        if loader:
            loader_module = file_loader.load_file(loader)
            config = loader_module.load(config_path, loader_param)
        else:
            import navi_default_loader
            config = navi_default_loader.load(config_path, loader_param)
        ret = json.dumps(config, indent=4)
        if not isinstance(config, dict):
            raise Exception("invalid config value, not a dict: %s" % ret)
        logging.info("result: " + ret)
        return True, ret, ""
    except Exception as e:
        errorMsg = "load config error, loader: %s, config_path: %s, param: %s, exception: %s, msg: %s" % (loader, config_path, loader_param, str(e), traceback.format_exc())
        return False, str(e), errorMsg
