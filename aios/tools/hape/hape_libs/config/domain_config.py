# -*- coding: utf-8 -*-

import os
from six.moves.configparser import ConfigParser
import jinja2
import json
import attr

from .global_config import GlobalConfig, default_variables
from .cluster_template import ClusterTemplate
from hape_libs.utils.template import *
from hape_libs.utils.shell import * 

class DomainConfig():
    def __init__(self, config_dir = None, render_template = True):
        
        ## get hape_conf
        self.config_dir =  config_dir
        if self.config_dir == None:
            self.config_dir = os.path.join(default_variables.hape_root, "hape_conf/default")
        else:
            self.config_dir = os.path.realpath(self.config_dir)
        Logger.info("Hape conf directory: {}".format(self.config_dir))
        
        ## load global config
        ## render every time
        self.global_config = self.load_global_config()
        Logger.info("Rendered hape global config: {}".format(attr.asdict(self.global_config)))
        
        ## load template
        ## render when start or restart
        self.template_config = ClusterTemplate(self.config_dir, self.global_config)
        if render_template:
            self.template_config.render()
            
        
    
    def load_global_config(self):
        raw_global_config = ConfigParser()
        raw_global_config.optionxform = str
        raw_global_config.read(os.path.join(self.config_dir, "global.conf"))
        raw_global_config_dict = {section: {option_name: option for option_name, option in raw_global_config.items(section)}
               for section in raw_global_config.sections()}
        
        ctx = {"default_variables": default_variables.to_dict()}
        env = jinja2.Environment(undefined = CustomUndefined)
        global_config_str = env.from_string(json.dumps(raw_global_config_dict)).render(**ctx)
        global_config_dict = json.loads(global_config_str)
        ret = GlobalConfig(**global_config_dict)
        return ret

        
        
    
