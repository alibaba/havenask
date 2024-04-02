# *-* coding:utf-8 *-*

import unittest

from hape_test_base import HapeTestBase
from hape_libs.common import HapeCommon
from hape_libs.testlib.main import test_main
from hape_libs.utils.retry import Retry

import time
import random

class HapePackTextIndexTest(unittest.TestCase, HapeTestBase):
    def setUp(self):
        self.init()

    def tearDown(self):
        self.destory()

    def test_pack_index(self):
        ## start
        self.assertTrue(self.hape_domain.start(HapeCommon.HAVENASK_KEY))

        ## table rw
        self._run_offline_tables_case("in1")

        self.assertTrue(self.hape_domain.stop(HapeCommon.HAVENASK_KEY))
        self.assertTrue(self.hape_domain.stop(HapeCommon.SWIFT_KEY))

        self.assertTrue(self.zk_wrapper.check_zk(path=self.domain_config.global_config.get_service_hippo_zk_address(HapeCommon.HAVENASK_KEY)))
        self.assertTrue(self.zk_wrapper.check_zk(path=self.domain_config.global_config.get_service_appmaster_zk_address(HapeCommon.HAVENASK_KEY)))

    def _run_offline_tables_case(self, table):
        self._run_offline_table_create(table)
        self._run_offline_table_rw(table)

        self._update_offline_table_schema(table)
        self._run_offline_table_rw2(table)

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
        address = self.hape_domain.suez_cluster.get_qrs_ips()[0] + ":" + self.domain_config.global_config.havenask.qrsHttpPort
        for i in range(len(datas)):
            query = '''SELECT {} FROM {} inner join {}_summary_ on {}.id={}_summary_.id where MATCHINDEX('default', 'aaa')&&kvpair=searchInfo:true;exec.leader.prefer.level:1;timeout:50000;databaseName:database;'''.format(fields_str, table, table, table, table)
            data = self.search_sql(address, query)
            self.assert_result(data, column_names, column_types, datas)

    def _run_offline_table_rw2(self, table):
        column_names = ['id', 'title', 'subject', 'hits2']
        column_types = ['uint32', 'multi_char', 'multi_char', 'uint32']
        time.sleep(5)

        datas = [
            [1, 'aaa\t \tbbb', '', 100],
            [2, '', 'aaa\t \tbbb', 200],
            [3, 'aaa\t \tbbb', 'aaa\t \tbbb', 300],
        ]

        fields_str = ','.join([table+"_summary_."+column_name for column_name in column_names])
        address = self.hape_domain.suez_cluster.get_qrs_ips()[0] + ":" + self.domain_config.global_config.havenask.qrsHttpPort
        for i in range(len(datas)):
            query = '''SELECT {} FROM {} inner join {}_summary_ on {}.id={}_summary_.id where MATCHINDEX('default', 'aaa')&&kvpair=searchInfo:true;exec.leader.prefer.level:1;timeout:50000;databaseName:database;'''.format(fields_str, table, table, table, table)
            data = self.search_sql(address, query)
            self.assert_result(data, column_names, column_types, datas)

    def _run_offline_table_create(self, table):
        self.assertTrue(self.hape_domain.create_table(table, 1, self.testdata_root + "/pack_schema.json", self.testdata_root + "/pack.data"))
        def check_offline_table_ready():
            table_status = self.hape_domain.get_table_status(specify_table_name=table)
            return table_status[table]["status"] == "READY"
        succ = Retry.retry(check_offline_table_ready, "Offline Table {} ready".format(table), limit=80)
        self.assertTrue(succ)
        self.assertTrue(self.hape_domain.suez_cluster.wait_cluster_ready())

    def _update_offline_table_schema(self, table):
        self.assertTrue(self.hape_domain.update_offline_table(table, 1, self.testdata_root + "/pack_schema2.json", self.testdata_root + "/pack2.data", 0))
        self.assertTrue(self.hape_domain.wait_new_generation_ready())
        def check_offline_table_ready():
            table_status = self.hape_domain.get_table_status(specify_table_name=table)
            return table_status[table]["status"] == "READY"
        succ = Retry.retry(check_offline_table_ready, "Offline Table {} ready".format(table), limit=80)
        self.assertTrue(succ)

test_main()
