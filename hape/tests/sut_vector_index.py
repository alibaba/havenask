# *-* coding:utf-8 *-*

import unittest

from hape_test_base import HapeTestBase
from hape_libs.common import HapeCommon
from hape_libs.testlib.main import test_main
from hape_libs.utils.retry import Retry
from hape_libs.utils.swift_writer import write_swift_message_bydict

import time
import random

class HapePackTextIndexTest(unittest.TestCase, HapeTestBase):
    def setUp(self):
        self.init(test_conf_dir="vector/vector_conf")
        
    def tearDown(self):
        self.destory()

    def test_vector_index(self):
        ## start
        self.assertTrue(self.hape_cluster.start_hippo_appmaster(HapeCommon.HAVENASK_KEY))
        
        ## table rw 
        self._run_direct_tables_case("in0")
        self._run_offline_tables_case("in1")
        
        self.assertTrue(self.hape_cluster.stop_hippo_appmaster(HapeCommon.HAVENASK_KEY))
        self.assertTrue(self.hape_cluster.stop_hippo_appmaster(HapeCommon.SWIFT_KEY))
            
        self.assertTrue(self.zk_wrapper.check_zk(path=self.hape_cluster.suez_appmaster.hippo_zk_full_path()))
        self.assertTrue(self.zk_wrapper.check_zk(path=self.hape_cluster.suez_appmaster.service_zk_full_path()))
    
    def _run_direct_tables_case(self, table):
        self._run_direct_table_create(table)
        self._run_direct_table_rw(table)

    def _run_offline_tables_case(self, table):
        self._run_offline_table_create(table)
        self._run_offline_table_rw(table)
                
    def _run_direct_table_rw(self, table):
        address = self.hape_cluster.suez_appmaster.get_qrs_ips()[0] + ":" + self.hape_cluster.suez_appmaster.get_qrs_httpport()
        vector = '0.57350874,0.20913178,-0.28007376,-0.008224763,0.032035876,-0.06109254,0.016208794,0.2546715,-0.5938332,0.2194785,-0.603397,0.095939286,-0.4343296,0.29292983,0.19570276,0.07006806,0.4219676,0.008796717,-0.1956801,0.10365201,-0.18756211,-0.1959297,-0.64994687,-0.0043594907,-0.12155021,-0.37211925,-0.033330534,0.37927315,0.14685613,0.14631498,0.2263155,0.020013737,-0.48688003,0.38279092,-0.14551091,-0.14309452,0.0836372,-0.2602252,-0.68886805,-0.18670991,0.026953692,-0.1361577,0.54825664,0.5495113,-0.11404511,0.020755235,-0.3592109,0.35327643,-0.17384957,0.06951289,-0.33219633,0.32536888,0.4178303,0.06838637,-0.32600296,-0.39717215,0.37040582,0.2577843,-0.022891786,-0.19590716,-0.34784496,0.45056996,-0.16618372,-0.048093718,-0.4501398,0.16741453,0.0021240758,-0.22811683,-0.30895764,-0.29165566,0.51220524,0.26545066,-0.46743664,-0.18925984,0.11317849,0.01674415,-0.41176468,-0.41503182,-0.52814317,-0.52559006,0.5788051,0.12608618,0.62300944,0.20152776,-0.39167175,0.099368036,0.34757188,-0.2860248,0.47917366,0.44290003,-0.3663736,0.074007034,-0.44493687,0.5212625,0.5742631,0.06809457,0.20434684,-0.2527957,0.08273791,-0.5062737,0.30677673,0.0037546167,0.53646725,0.6939553,-0.19357872,0.62397563,-0.25602227,-0.73345435,0.23730282,-0.11466419,-0.22964483,-0.18373214,-0.28414255,0.67296755,0.38798898,-0.0795316,0.050292946,0.51636153,-0.3399461,0.14268692,-0.5273593,-0.055386372,-0.29121432,-0.36689624,0.762627,0.14242941,0.14173277,0.08323621'
        datas = []
        num = 10
        for i in range(num):
            noise = ",".join([str(random.random()) for i in range(len(vector.split(",")))])
            datas.append((i,noise))
        datas.append((num, vector))
        for data in datas:
            idx, vec_str = data
            query = "insert into {} (id, vector) values({}, '{}')&&kvpair=databaseName:database;timeout:2000".format(table, idx, vec_str)
            data = self.search_sql(address, query)
            self.assert_result(data, ['ROWCOUNT'], ['int64'], [[1]])
        
        query = "select id,url from {} where MATCHINDEX('vector_index', '{}&n=1')&&kvpair=databaseName:database;timeout:2000".format(table, vector)
        data = self.search_sql(address, query)
        self.assert_result(data, ["id"], ["uint32"], [[num]])
            
    def _run_offline_table_rw(self, table):
        
        address = self.hape_cluster.suez_appmaster.get_qrs_ips()[0] + ":" + self.hape_cluster.suez_appmaster.get_qrs_httpport()
        vector = '0.788999,0.788999,-0.788999,-0.788999,0.788999,-0.788999,0.016208794,0.2546715,-0.5938332,0.2194785,-0.603397,0.095939286,-0.4343296,0.29292983,0.19570276,0.07006806,0.4219676,0.008796717,-0.1956801,0.10365201,-0.18756211,-0.1959297,-0.64994687,-0.0043594907,-0.12155021,-0.37211925,-0.033330534,0.37927315,0.14685613,0.14631498,0.2263155,0.020013737,-0.48688003,0.38279092,-0.14551091,-0.14309452,0.0836372,-0.2602252,-0.68886805,-0.18670991,0.026953692,-0.1361577,0.54825664,0.5495113,-0.11404511,0.020755235,-0.3592109,0.35327643,-0.17384957,0.06951289,-0.33219633,0.32536888,0.4178303,0.06838637,-0.32600296,-0.39717215,0.37040582,0.2577843,-0.022891786,-0.19590716,-0.34784496,0.45056996,-0.16618372,-0.048093718,-0.4501398,0.16741453,0.0021240758,-0.22811683,-0.30895764,-0.29165566,0.51220524,0.26545066,-0.46743664,-0.18925984,0.11317849,0.01674415,-0.41176468,-0.41503182,-0.52814317,-0.52559006,0.5788051,0.12608618,0.62300944,0.20152776,-0.39167175,0.099368036,0.34757188,-0.2860248,0.47917366,0.44290003,-0.3663736,0.074007034,-0.44493687,0.5212625,0.5742631,0.06809457,0.20434684,-0.2527957,0.08273791,-0.5062737,0.30677673,0.0037546167,0.53646725,0.6939553,-0.19357872,0.62397563,-0.25602227,-0.73345435,0.23730282,-0.11466419,-0.22964483,-0.18373214,-0.28414255,0.67296755,0.38798898,-0.0795316,0.050292946,0.51636153,-0.3399461,0.14268692,-0.5273593,-0.055386372,-0.29121432,-0.36689624,0.762627,0.14242941,0.14173277,0.08323621'
        query = "select id,url from {} where MATCHINDEX('vector_index', '{}&n=1')&&kvpair=databaseName:database;timeout:2000".format(table, vector)
        # data = {
        #     "id": 999,
        #     "vector": vector
        # }
        # write_swift_message_bydict(self.hape_cluster.swift_appmaster.service_zk_full_path(), table, 1, "id", data)
        response = self.search_sql(address, query)
        self.assert_result(response, ["id"], ["uint32"], [[1]])
    
    def _run_direct_table_create(self, table):
        self.assertTrue(self.hape_cluster.create_table(table, 1, self.testdata_root + "/vector/vector_schema.json", None))
        self.assertTrue(self.hape_cluster.wait_havenask_ready())
        
    def _run_offline_table_create(self, table):
        self.assertTrue(self.hape_cluster.create_table(table, 1, self.testdata_root + "/vector/vector_schema.json", self.testdata_root + "/vector/vector.data"))
        def check_offline_table_ready():
            table_status = self.hape_cluster.get_table_status(specify_table_name=table)
            return table_status[table]["status"] == "READY"
        succ = Retry.retry(check_offline_table_ready, "Offline Table {} ready".format(table), limit=80)
        self.assertTrue(succ)
        self.assertTrue(self.hape_cluster.wait_havenask_ready())
        
test_main()
