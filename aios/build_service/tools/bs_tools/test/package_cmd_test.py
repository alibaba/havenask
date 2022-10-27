#!/bin/env /usr/bin/python

import sys
import os

curPath = os.path.split(os.path.realpath(__file__))[0]
parentPath = curPath + "/../"
sys.path.append(parentPath)

import unittest
from package_cmd import *
from pm_run_delegate import *
from build_app_config import *
from tools_test_common import *

class PackageCmdTest(unittest.TestCase):
    def setUp(self):
        self.localPackagePath = TEST_DATA_PATH + '/package_cmd_test/local_package'
        self.addFailedPackage = TEST_DATA_PATH + '/package_cmd_test/add_failed_package'
        self.hadoopPackageName = TEST_DATA_TEMP_PATH + '/package_cmd_test/hadoop_package'

    def tearDown(self):
        pass

    def testAddPackageCmdDefaultValue(self):
        cmd = self.__makeFakePmRun(AddPackageCmd)
        cmd.parseParams(['-c', 'config_path', '-l', self.localPackagePath, '-t', 'apsara'])
        self.assertEqual('apsara', cmd.packageType)

    def testAddPackageCmdApsara(self):
        cmd = self.__makeFakePmRun(AddPackageCmd)
        cmd.parseParams(['-c', 'config_path', '-l', self.localPackagePath, '-t', 'apsara'])
        cmd.run()

    def testAddPackageApsaraFailed(self):
        cmd = self.__makeFakePmRun(AddPackageCmd)
        cmd.parseParams(['-c', 'config_path', '-l', self.addFailedPackage, '-t', 'apsara'])
        self.assertRaises(Exception, cmd.run)

    def testAddPackageCmdHadoop(self):
        cmd = self.__makeFakePmRun(AddPackageCmd)
        cmd.parseParams(['-c', 'config_path', '-l', self.localPackagePath, '-t', 'hadoop'])
        cmd.run()

    def testAddPackageHadoopFailed(self):
        self.hadoopPackageName = 'hdfs://not_exist/path/'
        cmd = self.__makeFakePmRun(AddPackageCmd)
        cmd.parseParams(['-c', 'config_path', '-l', self.addFailedPackage, '-t', 'hadoop'])
        self.assertRaises(Exception, cmd.run)

    def testAddPackageInvalidParam(self):
        cmd = self.__makeFakePmRun(AddPackageCmd)
        self.assertRaises(Exception, 
                          cmd.parseParams,
                          ['-l', self.localPackagePath, '-t', 'apsara'])
        self.assertRaises(Exception, 
                          cmd.parseParams,
                          ['-c', 'config_path', '-l', 'not_exist_file_path', '-t', 'apsara'])
        self.assertRaises(Exception, 
                          cmd.parseParams,
                          ['-c', 'config_path', '-l', self.localPackagePath, '-t', 'invalid_type'])
        self.assertRaises(Exception, 
                          cmd.parseParams,
                          ['-c', 'config_path', '-t', 'apsara'])

    def testRemovePackage(self):
        cmd = self.__makeFakePmRun(RemovePackageCmd)
        cmd.parseParams(['-c', 'config_path'])
        self.packageList.append('apsara_package_name')
        cmd.run()

    def testRemovePackageFailed(self):
        self.packageList = []
        cmd = self.__makeFakePmRun(RemovePackageCmd)
        cmd.parseParams(['-c', 'config_path'])
        self.assertRaises(Exception, cmd.run)

    def testListPackage(self):
        cmd = self.__makeFakePmRun(ListPackageCmd)
        cmd.parseParams(['-c', 'config_path'])
        cmd.run()
        
    def testListPackageFailed(self):
        cmd = self.__makeFakePmRun(ListPackageCmd, False)
        cmd.parseParams(['-c', 'config_path'])
        self.assertRaises(Exception, cmd.run)
        
    def __makeFakePmRun(self, cmdType, listPackageSucc=True):
        cmd = cmdType()
        setattr(cmd, 'baseInitMember', self.__fakeInitMember)
        self.listPackageSucc = listPackageSucc
        self.cmd = cmd
        return cmd

    def __fakeInitMember(self, options):
        self.cmd.buildAppConfig = build_app_config.BuildAppConfig()
        self.cmd.buildAppConfig.apsaraPackageName = 'apsara_package_name'
        self.cmd.buildAppConfig.hadoopPackageName = self.hadoopPackageName
        self.cmd.buildAppConfig.nuwaAddress = 'nuwa_address'
        self.cmd.pm = pm_run_delegate.PmRun('pm_run_exe',
                                              'nuwa_address')
        self.packageList = []
        self.cmd.pm.listPackageSucc = self.listPackageSucc
        setattr(self.cmd.pm, 'addPackage', self.fakeAddPackage)
        setattr(self.cmd.pm, 'removePackage', self.fakeRemovePackage)
        setattr(self.cmd.pm, 'listPackage', self.fakeListPackage)

    def fakeAddPackage(self, packageName, localPackage):
        if localPackage.endswith('add_failed_package'):
            raise Exception('add %s failed', packageName)
        self.packageList.append(packageName)

    def fakeRemovePackage(self, packageName):
        if packageName not in self.packageList:
            raise Exception('remove %s failed', packageName)
        self.packageList.remove(packageName)

    def fakeListPackage(self):
        if not self.listPackageSucc:
            return '', '', -1
        return '', '', 0

if __name__ == "__main__":
    unittest.main()
