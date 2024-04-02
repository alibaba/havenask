import unittest

from hape_test_base import HapeTestBase
from hape_libs.common import HapeCommon
from hape_libs.testlib.main import test_main

import time
import random

class HapeTableCatalogTest(unittest.TestCase, HapeTestBase):
    def setUp(self):
        self.init()
        
    def tearDown(self):
        self.destory()

    def test_table_catalog(self):
        ## start
        self.assertTrue(self.hape_domain.start(HapeCommon.HAVENASK_KEY))
        
        ## table rw
        self._run_direct_tables_case("in0")
        self._run_direct_tables_case("in1")
        self._run_direct_table_delete("in0")
        self._run_direct_table_rw("in1")
        catalog_manager = self.hape_domain.suez_cluster.get_catalog_manager()
        tables = list(catalog_manager.list_table().keys())
        self.assertTrue("in1" in tables)
        self.assertFalse("in2" in tables)
        
        
        self.assertTrue(self.hape_domain.stop(HapeCommon.HAVENASK_KEY))
        self.assertTrue(self.hape_domain.stop(HapeCommon.SWIFT_KEY))
            
        self.assertTrue(self.zk_wrapper.check_zk(path=self.domain_config.global_config.get_service_hippo_zk_address(HapeCommon.HAVENASK_KEY)))
        self.assertTrue(self.zk_wrapper.check_zk(path=self.domain_config.global_config.get_service_appmaster_zk_address(HapeCommon.HAVENASK_KEY)))
    
    def _run_direct_tables_case(self, table):
        self._run_direct_table_create(table)
        self._run_direct_table_rw(table)
        
    def _run_direct_table_rw(self, table):
        column_names = ['id', 'hits']
        column_types = ['uint32', 'uint32']
        datas = [
            [random.randint(0, 10000), random.randint(0, 10000)],
            [random.randint(0, 10000), random.randint(0, 10000)],
            [random.randint(0, 10000), random.randint(0, 10000)]
        ]
        fields_str = ','.join(column_names)
        address = self.hape_domain.suez_cluster.get_qrs_ips()[0] + ":" + self.domain_config.global_config.havenask.qrsHttpPort
        values_str = ','.join(['?' for i in range(len(column_names))])
        for i in range(len(datas)):
            dynamic_params = ','.join(['\\"{}\\"'.format(d) for d in datas[i]])
            query = '''INSERT into {} ({}) values({})&&kvpair=timeout:500000;searchInfo:true;exec.leader.prefer.level:1;databaseName:database;dynamic_params:[[{}]]'''.format(table, fields_str, values_str, dynamic_params)
            data = self.search_sql(address, query)
            self.assert_result(data, ['ROWCOUNT'], ['int64'], [[1]])
            
        time.sleep(5)
        
        for i in range(len(datas)):
            id = datas[i][0]
            query = '''SELECT {} FROM {} where id={}&&kvpair=searchInfo:true;exec.leader.prefer.level:1;timeout:50000;databaseName:database;'''.format(fields_str, table, id)
            data = self.search_sql(address, query)
            self.assert_result(data, column_names, column_types, [datas[i]])
    
    def _run_direct_table_delete(self, table):
        self.assertTrue(self.hape_domain.delete_table(table))
    
    def _run_direct_table_create(self, table):
        self.assertTrue(self.hape_domain.create_table(table, 1, self.testdata_root + "/in0_schema.json", None))
    
        
                 

test_main()
