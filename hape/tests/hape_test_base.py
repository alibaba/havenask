
import sys
import os
sys.path = [os.path.dirname(os.path.dirname(os.path.realpath(__file__)))] + sys.path

import socket
import requests
import json

from hape_libs.testlib.module_base import ModuleBase
from hape_libs.testlib.zk_wrapper import ZkWrapper
from hape_libs.config import ClusterConfig
from hape_libs.utils.logger import Logger
from hape_libs.common import HapeCommon
from hape_libs.testlib.hippo_plan_rewrite import HippoPlanRewrite
from hape_libs.clusters.hape_cluster import HapeCluster

import shutil
import time


class HapeTestBase(ModuleBase):
    port_used = set()
    @classmethod
    def setUpClass(cls):
        ModuleBase().remove_symlinks()
        
    def init(self, test_conf_dir=None):
        self.clean_workdir()
        
        self._init_dirs(None)
        self.zk_wrapper = ZkWrapper(path_root=self.workdir)
        self.assertTrue(self.zk_wrapper.start_zk())
        if test_conf_dir != None:
            self.cluster_config = ClusterConfig(self.testdata_root + "/" + test_conf_dir)
        else:
            self.cluster_config = ClusterConfig()
        self.cluster_config.set_domain_zk_root(self.zk_wrapper.host+"/havenask")
        self.cluster_config.set_processor_mode('local')
        self.cluster_config.set_default_var("hapeRoot", os.path.join(os.getcwd()) + "/hape")
        self.cluster_config.set_binary_path(self.workdir + "/install_root")
        self.cluster_config.set_default_var("userHome", self.workdir)
        self.cluster_config.set_processor_workdir(self.workdir)
        self.cluster_config.set_domain_config_param(HapeCommon.HAVENASK_KEY,  "allowMultiSlotInOne", "true")
        self.cluster_config.set_data_store_root(os.path.join(self.workdir, "havenask_data_store"))
        self.cluster_config.set_default_var("processorTag", self._testMethodName)
        self.cluster_config.init()
        
        cmds = [
            "suez_admin_worker", "ha_sql", "swift_admin", "swift_broker"   
        ]
        for cmd in cmds:
            self._pkill(cmd, extra=self._testMethodName)
        self.cluster_config.set_admin_http_port(str(self.get_free_port()))
        self.hape_cluster = HapeCluster(self.cluster_config)
        self.allocate_havenask_worker_resources(self.hape_cluster)
        
    def destory(self):
        self.zk_wrapper.stop()
        cmds = [
            "suez_admin_worker", "ha_sql", "swift_admin", "swift_broker"   
        ]
        for cmd in cmds:
            self._pkill(cmd, extra=self._testMethodName)
        

    def _init_dirs(self, work_dir):
        self.workdir = work_dir if work_dir is not None else self.__get_workdir()
        self.path_root = self.workdir + "/.."
        self.testdata_root = os.path.realpath(self.workdir + "../../../testdata")
        Logger.info("run test in path root {}".format(self.path_root))
        ModuleBase(path_root=self.path_root).remove_symlinks(os.path.join(self.workdir, 'install_root'))        

    def __get_workdir(self):
        workdir = os.path.join(os.getcwd()) + "/hape/tests/" + self._testMethodName
        if not os.path.exists(workdir):
            Logger.info("create workdir {}".format(workdir))
            os.makedirs(workdir)
        return workdir
        
    def clean_workdir(self):
        workdir = self.__get_workdir()
        if os.path.exists(workdir):
            shutil.rmtree(workdir)
    
    def allocate_havenask_worker_resources(self, hape_cluster): #type: (HapeCluster) -> None
        ori_func = hape_cluster.cluster_config.template_config.get_local_template_content
        qrs_ports = [self.get_free_port(),self.get_free_port(),self.get_free_port(),self.get_free_port()]
        searcher_ports = [self.get_free_port(),self.get_free_port(),self.get_free_port(),self.get_free_port()]
        def mock_local_template_content(path):
            if path == "havenask/hippo/qrs_hippo.json" or path == "havenask/hippo/searcher_hippo.json":
                hippo_plan = ori_func(path)
                hippo_plan = json.loads(hippo_plan)
                ports = qrs_ports if path == "havenask/hippo/qrs_hippo.json" else searcher_ports
                HippoPlanRewrite(hippo_plan).rewrite_ports(ports)
                return json.dumps(hippo_plan)
            else:
                return ori_func(path)

        hape_cluster.cluster_config.template_config.get_local_template_content = mock_local_template_content

    def get_free_port(self):
        limit = 50
        port = 45800
        for i in range(limit):
            sock = socket.socket()
            sock.bind(('', 0))
            port = sock.getsockname()[1]
            if port not in HapeTestBase.port_used: 
                break
        HapeTestBase.port_used.add(port)
        return port
    
    def search_sql(self, address, query, retry_count=5):
        query = query.replace('kvpair=', 'kvpair=formatType:full_json;')
        request = {'assemblyQuery': query}
        Logger.info('search sql request: ' + json.dumps(request, indent=4))
        for retry in range(retry_count):
            response = requests.post("http://{}/QrsService/searchSql".format(address), json=request)
            data = response.json()
            Logger.info('search sql response: ' + json.dumps(data, indent=4))
            errorcode = data["error_info"]['ErrorCode']
            if errorcode == 0:
                return data
        return data
        
    def assert_result(self, response, column_names, column_types, expect_datas):
        self.assertEqual('ERROR_NONE', response['error_info']['Error'])
        row_count = response['row_count']
        self.assertEqual(row_count, len(expect_datas))
        if row_count == 0:
            return
        sql_result = response['sql_result']
        col_count = len(column_names)
        self.assertEqual(col_count, len(column_types))
        for i in range(col_count):
            self.assertEqual(column_names[i], sql_result['column_name'][i])
            self.assertEqual(column_types[i], sql_result['column_type'][i])
        for i in range(row_count):
            for j in range(col_count):
                sql_data = sql_result['data'][i][j]
                expect_data = expect_datas[i][j]
                self.assertEqual(sql_data, expect_data)