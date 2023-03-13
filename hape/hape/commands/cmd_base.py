# -*- coding: utf-8 -*-

import os
from hape.utils.dict_file_util import DictFileUtil
from hape.utils.logger import Logger
hape_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

class CommandBase:
    def __init__(self, hape_config_path):
        self._hape_config_path = os.path.abspath(hape_config_path)
        Logger.info("load hape config, path:{}".format(self._hape_config_path))
        global_config = DictFileUtil.read(os.path.join(self._hape_config_path, "global.conf"))
        self._global_config = global_config    
        self._admin_config = global_config["hape-admin"]
        self._admin_workdir = self._admin_config["workdir"]
        Logger.init_stdout_logger(self._admin_config["log-cmd-debug"] == "True")
    
    