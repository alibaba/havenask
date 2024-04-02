import os
import jinja2
import hashlib
import attr
import json
import base64

from hape_libs.utils.fs_wrapper import FsWrapper
from hape_libs.utils.logger import Logger
from hape_libs.utils.template import CustomUndefined, TemporaryDirectory
from .global_config import GlobalConfig



def read_file_as_base64(filepath):
    with open(filepath, "r") as f:
        original_text = f.read()
        base64_encoded_text = base64.b64encode(original_text)
        return base64_encoded_text

class ClusterTemplate():
    def __init__(self, config_dir, global_config): #type: (str, GlobalConfig) -> None
        self._config_dir = config_dir
        self._global_config = global_config
        self._qrs_hippo = {}
        self._searcher_hippo = {}
        self._zk_config = {}
        self._swift_hippo = {}
        self._swift_conf = ""
        self.md5_signature = ""
        self.rel_path_to_content = {}
    
    def render(self):
        self._templ_fs = FsWrapper(os.path.join(self._global_config.common.dataStoreRoot, "templates"), extend_attrs={
                            "binary_path" :self._global_config.common.binaryPath, 
                            "hadoop_home": self._global_config.common.hadoopHome
                            })
        if not self._templ_fs.exists(""):
            self._templ_fs.mkdir("")
            
        root = os.path.join(self._config_dir, "cluster_templates")
        for dirpath, dirnames, filenames in os.walk(root):
            for filename in filenames:
                relpath = os.path.relpath(os.path.join(dirpath, filename), root)
                if relpath.endswith(".yaml") or relpath.endswith(".conf") or relpath.endswith(".py") or relpath.endswith(".json"):
                    self._render_file(root, relpath)
                    
    def _render_file(self, root, relpath):
        fullpath = os.path.join(root, relpath)
        loader = jinja2.FileSystemLoader(os.path.dirname(fullpath))
        env = jinja2.Environment(undefined = CustomUndefined, loader = loader)
        env.globals['read_file_as_base64'] = read_file_as_base64
        template = env.get_template(os.path.basename(fullpath))
        output = template.render(**attr.asdict(self._global_config))
        self.rel_path_to_content[relpath] = output
        
    def upload_templ(self, dirname):
        hash_md5 = hashlib.md5()
        
        for rel_path, content in self.rel_path_to_content.items():
            if rel_path.find(dirname) != 0:
                continue
            try:
                hash_md5.update(content.encode('utf-8'))
            except:
                Logger.warning("Failed to encode file {}".format(rel_path))
        self.md5_signature = hash_md5.hexdigest()
        
        md5_dirname = os.path.join(self.md5_signature, dirname)
        addr = self._templ_fs.complete_path(md5_dirname)
        if not self._templ_fs.exists(md5_dirname):
            with TemporaryDirectory() as tmp_dir:
                tmp_md5dirname = os.path.join(tmp_dir, self.md5_signature)
                tmp_fs = FsWrapper(tmp_md5dirname)
                for rel_path, content in self.rel_path_to_content.items():
                    if rel_path.find(dirname) != 0:
                        continue
                    tmp_fs.write(content, rel_path)
                self._templ_fs.put(os.path.join(tmp_dir, md5_dirname), addr)
            
        Logger.debug("Template uploaded in {}".format(addr))
        return addr
            
    def upload_offline_table_templ(self):
        return self.upload_templ("offline_table")
    
    def upload_direct_table_templ(self):
        return self.upload_templ("direct_table")
        
    def get_rendered_appmaster_plan(self, key):
        raw_appmaster_plan = self.rel_path_to_content["appmaster/{}_appmaster.json".format(key)]
        Logger.debug("Loads {} appmaster plan: {}".format(key, raw_appmaster_plan))
        raw_appmaster_plan = json.loads(raw_appmaster_plan)
        return raw_appmaster_plan

    def get_rendered_c2_plan(self):
        return self.rel_path_to_content["appmaster/c2.yaml"]

    