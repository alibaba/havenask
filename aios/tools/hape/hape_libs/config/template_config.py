
import time
import sys
import os
import traceback
import hashlib
import threading

from hape_libs.utils.fs_wrapper import FsWrapper
from hape_libs.utils.logger import Logger
from hape_libs.utils.template import TemplateRender
from hape_libs.common import *

class TemplateConfig():
    def __init__(self, cluster_config, render_variables):
        self._need_to_render = [
            "havenask/offline_table/bs_hippo.json",
            "havenask/offline_table/analyzer.json",
            "havenask/direct_table/biz_config/analyzer.json",
            "havenask/direct_table/analyzer.json",
            "havenask/hippo/qrs_hippo.json",
            "havenask/hippo/searcher_hippo.json",
            "swift/config/swift_hippo.json",
            "swift/config/swift.conf"
        ]
        self._cluster_config = cluster_config
        self._source_fs_wrapper = FsWrapper(self._cluster_config.config_dir + "/cluster_templates")
        self._render_variables = render_variables
        self._config_name = self._cluster_config.config_dir.split("/")[-1]
        self.md5 = ""
        self.uploaded = False
        
    def render(self):
        paths = []
        datas = []
        
        
        ## render 
        for path in self._source_fs_wrapper.listdir(""):
            data = self._source_fs_wrapper.get(path)
            if path in self._need_to_render:
                data = TemplateRender.render(data, self._render_variables)
            datas.append(data)
            paths.append(path)

        if self._cluster_config.domain_config.has_section(HapeCommon.BS_KEY):
            bs_hippo_zk = os.path.join(self._cluster_config.get_domain_config_param(HapeCommon.COMMON_CONF_KEY, "domainZkRoot"), 
                                self._cluster_config.get_domain_config_param(HapeCommon.BS_KEY, "serviceName")) + "_hippo"
            path = "havenask/offline_table/build_app.json"
            data = self._source_fs_wrapper.get(path)  
            data = TemplateRender.render(data, {"hippo_root": bs_hippo_zk})
            datas.append(data)
            paths.append(path)
        
        raw_str = str("".join(datas))
        self.md5 = hashlib.md5(raw_str.encode('utf-8')).hexdigest()
        
        self.template_fs_wrapper = FsWrapper(self._cluster_config.get_templates_root(), extend_attrs={
                            "binary_path" :self._cluster_config.get_binary_path(), 
                            "hadoop_home": self._cluster_config.get_domain_config_param(HapeCommon.COMMON_CONF_KEY, "hadoopHome")
                            })
        
        if not self.template_fs_wrapper.exists(""):
            self.template_fs_wrapper.mkdir("")

        template_tempdir = os.path.join(self._cluster_config.get_default_var("userHome"), ".hape_local_templates_store")
        tempdir_wrapper = FsWrapper(template_tempdir)
        if tempdir_wrapper.exists(""):
            tempdir_wrapper.rm("")
            
        self.template_tempdir = os.path.join(template_tempdir, self.get_config_name())
         
        self.tempdir_wrapper = FsWrapper(self.template_tempdir)   
        for data, path in zip(datas, paths):
            self.tempdir_wrapper.write(data, path)
                
        Logger.info("Render template config [{}] to [{}]".format(self._cluster_config.config_dir, self.template_tempdir))
            
    def get_bs_template_path(self):
        return os.path.join(self._cluster_config.get_templates_root(), self.get_config_name(), "havenask/offline_table")
    
    def get_direct_write_template_path(self):
        return os.path.join(self._cluster_config.get_templates_root(), self.get_config_name(), "havenask/direct_table")
    
    def get_local_template_content(self, path):
        Logger.debug("Read template content from dir:{} path:{}".format(self._cluster_config.get_templates_root(), path))
        return self.tempdir_wrapper.get(path)
    
    def get_config_name(self):
        return self._config_name + "." + self.md5
    
    def upload(self):
        if not self.uploaded:
            Logger.info("Upload template")
            self.template_fs_wrapper.put(self.template_tempdir, "", filename=self.get_config_name())
            self.uploaded = True
    