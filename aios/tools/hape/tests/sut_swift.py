import unittest

from hape_test_base import HapeTestBase
from hape_libs.clusters import HapeCluster
from hape_libs.common import HapeCommon
from hape_libs.testlib.main import test_main
from hape_libs.utils.shell import LocalShell
from hape_libs.utils.logger import Logger


class HapeSwiftTest(unittest.TestCase, HapeTestBase):
    def setUp(self):
        self.clean_workdir()
        self.init()
        self._pkill("swift_admin")
        self._pkill("swift_broker")
        
    def tearDown(self):
        self.zk_wrapper.stop()
        self._pkill("swift_admin")
        self._pkill("swift_broker")

    def test_swift_start_stop(self):
        ## start
        hape_cluster = HapeCluster(self.cluster_config)
        self.assertTrue(hape_cluster.start_hippo_appmaster(HapeCommon.SWIFT_KEY))
        
        ## stop
        self.assertTrue(hape_cluster.stop_hippo_appmaster(HapeCommon.SWIFT_KEY))
        self.assertTrue(self.zk_wrapper.check_zk(path=hape_cluster.swift_appmaster.hippo_zk_full_path()))
        self.assertTrue(self.zk_wrapper.check_zk(path=hape_cluster.swift_appmaster.service_zk_full_path()))
        
    def test_swift_start_delete(self):
        ## start
        hape_cluster = HapeCluster(self.cluster_config)
        self.assertTrue(hape_cluster.start_hippo_appmaster(HapeCommon.SWIFT_KEY))
        
        ## delete
        self.assertTrue(hape_cluster.stop_hippo_appmaster(HapeCommon.SWIFT_KEY, is_delete=True))
        self.assertFalse(self.zk_wrapper.check_zk(path=hape_cluster.swift_appmaster.hippo_zk_full_path()))
        self.assertFalse(self.zk_wrapper.check_zk(path=hape_cluster.swift_appmaster.service_zk_full_path()))

    

test_main()
