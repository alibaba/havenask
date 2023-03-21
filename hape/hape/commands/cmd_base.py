# -*- coding: utf-8 -*-

import os
from hape.utils.dict_file_util import DictFileUtil
from hape.utils.logger import Logger
hape_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

class CommandBase:
    def init_member(self, hape_config_path):
        self._hape_config_path = os.path.abspath(hape_config_path)
        Logger.info("load hape config, path:{}".format(self._hape_config_path))
        global_config = DictFileUtil.read(os.path.join(self._hape_config_path, "global.conf"))
        self._global_config = global_config    
        self._admin_config = global_config["hape-admin"]
        self._admin_workdir = self._admin_config["workdir"]
        Logger.init_stdout_logger(self._admin_config["enable-cmd-debug"] == "True")
    
    @staticmethod
    def _strip_domain_name(config_path):
        if config_path.endswith("/"):
            config_path = config_path[:-1]
            
        name = os.path.basename(config_path)
        index = name.find("_hape_config")
        if index == -1:
            raise ValueError("config path must ends with '_hape_config'")
        domain_name = name[:index]
        Logger.info("parse domain name from {} as {}".format(config_path, domain_name))
        return domain_name
    