import sys
import os
import logging
this_dir = os.path.dirname(__file__)
parent_dir = os.path.join(this_dir, "..")
dirs = os.listdir(parent_dir)
for d in dirs:
    sys.path.append(os.path.join(parent_dir, d))
formatStr = '[%(asctime)s] [%(process)s] [%(filename)s:%(lineno)d] [%(levelname)s] - [%(message)s]'
datefmtStr = '%m-%d %H:%M:%S'
if not os.path.exists('./logs'):
    os.mkdir('./logs')
logging.basicConfig(filename='./logs/config_loader.log', level=logging.INFO, format = formatStr, datefmt = datefmtStr)
# logging.info("new sys.path: " + str(sys.path))
import json
import traceback
import file_loader

def navi_config(loader, config_path, loader_param):
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
