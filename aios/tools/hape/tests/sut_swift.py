import unittest

from hape_test_base import HapeTestBase
from hape_libs.domain import HavenaskDomain
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
        hape_domain = HavenaskDomain(self.domain_config)
        self.assertTrue(hape_domain.start(HapeCommon.SWIFT_KEY))
        
        ## stop
        self.assertTrue(hape_domain.stop(HapeCommon.SWIFT_KEY))
        self.assertTrue(self.zk_wrapper.check_zk(path=self.domain_config.global_config.get_service_hippo_zk_address(HapeCommon.SWIFT_KEY)))
        self.assertTrue(self.zk_wrapper.check_zk(path=self.domain_config.global_config.get_service_appmaster_zk_address(HapeCommon.SWIFT_KEY)))
        
    def test_swift_start_delete(self):
        ## start
        hape_domain = HavenaskDomain(self.domain_config)
        self.assertTrue(hape_domain.start(HapeCommon.SWIFT_KEY))
        
        ## delete
        self.assertTrue(hape_domain.stop(HapeCommon.SWIFT_KEY, is_delete=True))
        self.assertFalse(self.zk_wrapper.check_zk(path=self.domain_config.global_config.get_service_hippo_zk_address(HapeCommon.SWIFT_KEY)))
        self.assertFalse(self.zk_wrapper.check_zk(path=self.domain_config.global_config.get_service_appmaster_zk_address(HapeCommon.SWIFT_KEY)))

    

test_main()
