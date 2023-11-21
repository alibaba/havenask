# *-* coding:utf-8 *-*

import unittest

from hape_test_base import HapeTestBase
from hape_libs.common import HapeCommon
from hape_libs.testlib.main import test_main
from hape_libs.utils.retry import Retry
from hape_libs.utils.shell import LocalShell

import time
import random

class HapePackTextIndexTest(unittest.TestCase, HapeTestBase):
    def setUp(self):
        self.init()

    def tearDown(self):
        self.destory()

    def test_pack_index(self):
        ## start
        self.assertTrue(self.hape_cluster.start_hippo_appmaster(HapeCommon.HAVENASK_KEY))

        ## table rw
        self._run_offline_tables_case("in1")

        self.assertTrue(self.hape_cluster.stop_hippo_appmaster(HapeCommon.HAVENASK_KEY))
        self.assertTrue(self.hape_cluster.stop_hippo_appmaster(HapeCommon.SWIFT_KEY))

        self.assertTrue(self.zk_wrapper.check_zk(path=self.hape_cluster.suez_appmaster.hippo_zk_full_path()))
        self.assertTrue(self.zk_wrapper.check_zk(path=self.hape_cluster.suez_appmaster.service_zk_full_path()))

    def _run_offline_tables_case(self, table):
        self._run_offline_table_create(table)
        self._run_offline_table_rw(table)
        self._update_qrs_hippo_config(table)
        self._update_searcher_hippo_config(table)
        self.assertTrue(self.hape_cluster.wait_havenask_ready())
        self._run_offline_table_rw(table)

    def _run_offline_table_rw(self, table):
        column_names = ['id', 'title', 'subject']
        column_types = ['uint32', 'multi_char', 'multi_char']
        time.sleep(5)

        datas = [
            [1, 'aaa\t \tbbb', ''],
            [2, '', 'aaa\t \tbbb'],
            [3, 'aaa\t \tbbb', 'aaa\t \tbbb'],
        ]

        fields_str = ','.join([table+"_summary_."+column_name for column_name in column_names])
        address = self.hape_cluster.suez_appmaster.get_qrs_ips()[0] + ":" + self.hape_cluster.suez_appmaster.get_qrs_httpport()
        for i in range(len(datas)):
            query = '''SELECT {} FROM {} inner join {}_summary_ on {}.id={}_summary_.id where MATCHINDEX('default', 'aaa')&&kvpair=searchInfo:true;exec.leader.prefer.level:1;timeout:50000;databaseName:database;'''.format(fields_str, table, table, table, table)
            data = self.search_sql(address, query)
            self.assert_result(data, column_names, column_types, datas)

    def _run_offline_table_create(self, table):
        self.assertTrue(self.hape_cluster.create_table(table, 1, self.testdata_root + "/pack_schema.json", self.testdata_root + "/pack.data"))
        def check_offline_table_ready():
            table_status = self.hape_cluster.get_table_status(specify_table_name=table)
            return table_status[table]["status"] == "READY"
        succ = Retry.retry(check_offline_table_ready, "Offline Table {} ready".format(table), limit=80)
        self.assertTrue(succ)
        self.assertTrue(self.hape_cluster.wait_havenask_ready())

    def _update_qrs_hippo_config(self, table):
        role_type = 'qrs'
        hippo_config = self.hape_cluster.get_default_hippo_config(role_type)
        process_args = hippo_config["role_desc"]["processInfos"][0]["args"]
        for arg in process_args:
            if arg["key"] == "--env" and arg["value"].startswith("gigGrpcThreadNum="):
                arg["value"] = "gigGrpcThreadNum=6"

        self.hape_cluster.update_default_cluster(hippo_config, role_type)
        local_shell = LocalShell()
        param_str = "gigGrpcThreadNum=6"
        def check_update_param_ready():
            cmd = 'ps auxwww|grep -v grep|grep ha_sql|grep sut_update_hippo_config|grep qrs|grep gig|grep "{}"'.format(param_str)
            out = local_shell.execute_command(cmd)
            return len(out) != 0
        succ = Retry.retry(check_update_param_ready, "update qrs hippo config ready", limit=30)
        self.assertTrue(succ)

    def _update_searcher_hippo_config(self, table):
        role_type = 'searcher'
        hippo_config = self.hape_cluster.get_default_hippo_config(role_type)
        process_args = hippo_config["role_desc"]["processInfos"][0]["args"]
        for arg in process_args:
            if arg["key"] == "--env" and arg["value"].startswith("gigGrpcThreadNum="):
                arg["value"] = "gigGrpcThreadNum=4"

        self.hape_cluster.update_default_cluster(hippo_config, role_type)
        local_shell = LocalShell()
        param_str = "gigGrpcThreadNum=4"
        def check_update_param_ready():
            cmd = 'ps auxwww|grep -v grep|grep ha_sql|grep sut_update_hippo_config|grep searcher|grep gig|grep "{}"'.format(param_str)
            out = local_shell.execute_command(cmd)
            return len(out) != 0
        succ = Retry.retry(check_update_param_ready, "update searcher hippo config ready", limit=30)
        self.assertTrue(succ)

test_main()
