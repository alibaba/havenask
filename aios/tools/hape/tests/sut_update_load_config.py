# *-* coding:utf-8 *-*

import unittest

from hape_test_base import HapeTestBase
from hape_libs.common import HapeCommon
from hape_libs.testlib.main import test_main
from hape_libs.utils.retry import Retry
from hape_libs.utils.shell import LocalShell

import time
import json
import os

class HapePackTextIndexTest(unittest.TestCase, HapeTestBase):
    def setUp(self):
        self.init()

    def tearDown(self):
        self.destory()

    def test_pack_index(self):
        ## start
        self.assertTrue(self.hape_cluster.start_hippo_appmaster(HapeCommon.HAVENASK_KEY))

        ## table rw
        self._run_direct_tables_case("in0")
        self._run_offline_tables_case("in1")

        self.assertTrue(self.hape_cluster.stop_hippo_appmaster(HapeCommon.HAVENASK_KEY))
        self.assertTrue(self.hape_cluster.stop_hippo_appmaster(HapeCommon.SWIFT_KEY))

        self.assertTrue(self.zk_wrapper.check_zk(path=self.hape_cluster.suez_appmaster.hippo_zk_full_path()))
        self.assertTrue(self.zk_wrapper.check_zk(path=self.hape_cluster.suez_appmaster.service_zk_full_path()))

    def _run_offline_tables_case(self, table):
        self._run_offline_table_create(table)
        self._run_offline_table_rw(table)

        self._update_load_strategy_1(table)
        self._check_lock_mem("8 kB")
        self._run_offline_table_rw(table)

        self._update_load_strategy_2(table)
        self._check_lock_mem("64 kB")
        self._run_offline_table_rw(table)

    def _run_direct_tables_case(self, table):
        self._run_direct_table_create(table)
        self._run_direct_table_rw(table)

        self._update_load_strategy_1(table)
        self._check_max_realtime_memory_use("1.3G")
        self.assertTrue(self.hape_cluster.wait_havenask_ready())
        self._run_direct_table_rw(table)

        self._update_load_strategy_2(table)
        self._check_max_realtime_memory_use("2.1G")
        self.assertTrue(self.hape_cluster.wait_havenask_ready())
        self._run_direct_table_rw(table)

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

    def _run_direct_table_create(self, table):
        self.assertTrue(self.hape_cluster.create_table(table, 1, self.testdata_root + "/pack_schema.json", None))
        self.assertTrue(self.hape_cluster.wait_havenask_ready())

    def _run_direct_table_rw(self, table):
        column_names = ['id', 'title', 'subject']
        column_types = ['uint32', 'multi_char', 'multi_char']
        idxs = [1234, 1235, 1236]
        datas = [
            [idxs[0], 'aaa bbb', ''],
            [idxs[1], '', 'aaa bbb'],
            [idxs[2], 'aaa bbb', 'aaa bbb'],
        ]
        fields_str = ','.join(column_names)
        address = self.hape_cluster.suez_appmaster.get_qrs_ips()[0] + ":" + self.hape_cluster.suez_appmaster.get_qrs_httpport()
        values_str = ','.join(['?' for i in range(len(column_names))])
        for i in range(len(datas)):
            dynamic_params = ','.join(['\\"{}\\"'.format(d) for d in datas[i]])
            query = '''INSERT into {} ({}) values({})&&kvpair=timeout:500000;searchInfo:true;exec.leader.prefer.level:1;databaseName:database;dynamic_params:[[{}]]'''.format(table, fields_str, values_str, dynamic_params)
            data = self.search_sql(address, query)
            self.assert_result(data, ['ROWCOUNT'], ['int64'], [[1]])

        time.sleep(5)

        datas = [
            [idxs[0], 'aaa\t \tbbb', ''],
            [idxs[1], '', 'aaa\t \tbbb'],
            [idxs[2], 'aaa\t \tbbb', 'aaa\t \tbbb'],
        ]
        fields_str = ','.join([table+"_summary_."+column_name for column_name in column_names])
        for i in range(len(datas)):
            query = '''SELECT {} FROM {} inner join {}_summary_ on {}.id={}_summary_.id where MATCHINDEX('default', 'aaa')&&kvpair=searchInfo:true;exec.leader.prefer.level:1;timeout:50000;databaseName:database;'''.format(fields_str, table, table, table, table)
            data = self.search_sql(address, query)
            self.assert_result(data, column_names, column_types, datas)

    def _update_load_strategy_1(self, table):
        online_index_config = {}
        online_index_config["build_config"] = {}
        online_index_config["build_config"]["build_total_memory"] = 1024
        online_index_config["max_realtime_memory_use"] = 4096
        # lock 8 kB
        dictionary_load_config = json.loads('''{
            "name": "summary_lock_value",
            "file_patterns": [ "_SUMMARY_" ],
            "load_strategy": "mmap",
            "load_strategy_param": { "lock": true }
        }''')
        online_index_config["load_config"] = [dictionary_load_config]
        self.assertTrue(self.hape_cluster.create_or_update_load_strategy(table, json.dumps(online_index_config)))
        self.assertTrue(self.hape_cluster.wait_havenask_ready())

    def _update_load_strategy_2(self, table):
        online_index_config = {}
        online_index_config["build_config"] = {}
        online_index_config["build_config"]["build_total_memory"] = 3456
        online_index_config["max_realtime_memory_use"] = 6543
        # lock 64 kB
        dictionary_load_config = json.loads('''{
            "name": "all_lock_value",
            "file_patterns": [ ".*" ],
            "load_strategy": "mmap",
            "load_strategy_param": { "lock": true }
        }''')
        online_index_config["load_config"] = [dictionary_load_config]
        self.assertTrue(self.hape_cluster.create_or_update_load_strategy(table, json.dumps(online_index_config)))
        self.assertTrue(self.hape_cluster.wait_havenask_ready())

    def getSearcherPid(self):
        cmd = 'pgrep ha_sql|grep -v grep|xargs pwdx|grep sut_update_load_config|grep database_partition_0|cut -d: -f1'
        out = LocalShell().execute_command(cmd)
        pid = int(out.strip())
        return pid

    def _check_lock_mem(self, lock_mem_str):
        searcher_pid = self.getSearcherPid()
        def check_update_load_strategy_ready():
            cmd = 'cat /proc/{}/status|grep -i VmLck|cut -d: -f2'.format(searcher_pid)
            out = LocalShell().execute_command(cmd)
            lock_mem = out.strip()
            return lock_mem == lock_mem_str
        succ = Retry.retry(check_update_load_strategy_ready,
                           "update searcher load strategy ready",
                           limit=30)
        self.assertTrue(succ)

    def _check_max_realtime_memory_use(self, mem_str):# mem_str equal to max_realtime_memory_use/3
        searcher_pid = self.getSearcherPid()
        sql_log_file_path = os.path.join(self.workdir, 'havenask-sql-local_database.database_partition_0/ha_sql/logs/sql.log')
        sql_begin_line = sum(1 for _ in open(sql_log_file_path))

        index_ready_log_str = "using suggest building segment memory \[{}".format(mem_str)
        indexlib_log_file_path = os.path.join(self.workdir, 'havenask-sql-local_database.database_partition_0/ha_sql/logs/index_bs.log')
        def check_update_load_strategy_ready():
            cmd = 'tail -200 {}|grep "{}"'.format(indexlib_log_file_path, index_ready_log_str)
            out = LocalShell().execute_command(cmd)
            return len(out.strip()) != 0
        succ = Retry.retry(check_update_load_strategy_ready,
                           "update searcher load strategy ready",
                           limit=30)
        self.assertTrue(succ)
        self._check_reach_target(sql_log_file_path, sql_begin_line)

    def _check_reach_target(self, sql_log_file_path, begin_line=0):
        def check_reach_target():
            cmd = 'tail -n +{} {}|grep "{}"'.format(begin_line, sql_log_file_path, reach_target_str)
            out = LocalShell().execute_command(cmd)
            return len(out.strip()) != 0
        reach_target_str = "searchUpdateResult: UR_REACH_TARGET"
        succ = Retry.retry(check_reach_target,
                           "check reach target ready",
                           limit=30)
        self.assertTrue(succ)
        reach_target_str = "tableUpdateResult: UR_REACH_TARGET"
        succ = Retry.retry(check_reach_target,
                           "check reach target ready",
                           limit=30)
        self.assertTrue(succ)
        time.sleep(10)

test_main()
