#!/usr/bin/python2.7

import sys
import os
hape_root = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path = [hape_root] + sys.path
workdir = os.path.join(os.getcwd()) + "/aios/tools/hape/tests/"
python_package = os.path.join(workdir, "install_root/usr/local/lib/python/site-packages")
sys.path = [python_package] + sys.path

import socket
import requests
import json


from hape_libs.testlib.module_base import ModuleBase
from hape_libs.testlib.zk_wrapper import ZkWrapper
from hape_libs.config import DomainConfig
from hape_libs.utils.logger import Logger
from hape_libs.utils.carbon_plan import *
from hape_libs.domain import HavenaskDomain
from hape_libs.config.global_config import default_variables

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
            self.domain_config = DomainConfig(self.testdata_root + "/" + "../" + test_conf_dir, render_template= False)
        else:
            self.domain_config = DomainConfig(self.workdir + "../../../hape_conf/proc", render_template = False)
        default_variables.user_home = self.workdir
        global_config = self.domain_config.global_config
        global_config.common.domainZkRoot = self.zk_wrapper.host+"/havenask"
        global_config.common.binaryPath = self.workdir + "/install_root"
        global_config.common.dataStoreRoot = os.path.join(self.workdir, "havenask_data_store")
        print("make test", global_config.common.dataStoreRoot)
        global_config.fill()
        
        cmds = [
            "suez_admin_worker", "ha_sql", "swift_admin", "swift_broker"   
        ]
        for cmd in cmds:
            self._pkill(cmd, extra=self._testMethodName)
        global_config.havenask.adminHttpPort = str(self.get_free_port())
        self.hape_domain = HavenaskDomain(self.domain_config)
        self.domain_config.template_config.render()
        self.rewrite_ports(self.domain_config)
        
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
        workdir = os.path.join(os.getcwd()) + "/aios/tools/hape/tests/" + self._testMethodName
        if not os.path.exists(workdir):
            Logger.info("create workdir {}".format(workdir))
            os.makedirs(workdir)
        return workdir
        
    def clean_workdir(self):
        workdir = self.__get_workdir()
        if os.path.exists(workdir):
            shutil.rmtree(workdir)
    
    def rewrite_ports(self, domain_config): #type: (DomainConfig) -> None
        qrs_plan_path = "hippo/qrs_hippo.json"
        searcher_plan_path = "hippo/searcher_hippo.json"
        for path in [qrs_plan_path, searcher_plan_path]:
            ports = [self.get_free_port(),self.get_free_port(),self.get_free_port(),self.get_free_port(), self.get_free_port()]
            raw_plan = domain_config.template_config.rel_path_to_content[path]
            raw_plan = json.loads(raw_plan)
            plan = CarbonPlan(**raw_plan)
            kkey_seps = [("--port", "arpc", ":"), ("--port", "http", ":"), ("--env", "httpPort", "="), ("--env", "gigGrpcPort", "="), ("--env", "port", "=")]
            for index, (key, subkey, subkey_sep) in enumerate(kkey_seps):
                plan.rewrite_kkv_args(key, subkey, subkey_sep, str(ports[index]))
            plan.rewrite_health_config("PORT", str(ports[2]))
            domain_config.template_config.rel_path_to_content[path] = json.dumps(attr.asdict(plan), indent=4)
            if path == qrs_plan_path:
                domain_config.global_config.havenask.qrsHttpPort = str(ports[2])

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