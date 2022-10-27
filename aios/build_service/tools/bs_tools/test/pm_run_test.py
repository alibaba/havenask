#!/bin/env /usr/bin/python

import sys
import os
import unittest
curPath = os.path.split(os.path.realpath(__file__))[0]
parentPath = curPath + "/../"
sys.path.append(parentPath)

import pm_run_delegate
import fake_process

class PmRunTest(unittest.TestCase):
    def setUp(self):
        self.pmRunExe = 'pm_run_exe'
        self.nuwaAddress = 'nuwa_address'
        self.pmRun = pm_run_delegate.PmRun(self.pmRunExe, self.nuwaAddress)

    def tearDown(self):
        pass

    def testAddPackage(self):
        addPackageCmd = 'pm_run_exe -pm nuwa_address AddPackage package_name meta /local/package.tar.gz'
        data = 'error: [NA]\nreply: [0]\n'
        error = ""
        code = 0
        resultDict = {addPackageCmd : (data, error, code)}
        self.pmRun.process = fake_process.FakeProcess(resultDict)
        self.pmRun.addPackage('package_name', '/local/package.tar.gz')

    def testAddPackageFailed(self):
        addPackageCmd = 'pm_run_exe -pm nuwa_address AddPackage package_name meta /local/package.tar.gz'
        data = 'reply: [-90008]\nerror: [package exists!]\n'
        error = ""
        code = 0
        resultDict = {addPackageCmd : (data, error, code)}
        self.pmRun.process = fake_process.FakeProcess(resultDict)
        self.assertRaises(Exception, self.pmRun.addPackage,'package_name', '/local/package.tar.gz')


    def testRemovePackage(self):
        removePackageCmd = 'pm_run_exe -pm nuwa_address RemovePackage package_name'
        data = 'reply: [0]\nerror: [NA]\n'
        error = ""
        code = 0
        resultDict = {removePackageCmd : (data, error, code)}
        self.pmRun.process = fake_process.FakeProcess(resultDict)
        self.pmRun.removePackage('package_name')

    def testRemovePackageFailed(self):
        removePackageCmd = 'pm_run_exe -pm nuwa_address RemovePackage package_name'
        data = 'reply: [-90008]\nerror: [Package not found]\n'
        error = ""
        code = 0
        resultDict = {removePackageCmd : (data, error, code)}
        self.pmRun.process = fake_process.FakeProcess(resultDict)
        self.assertRaises(Exception, self.pmRun.removePackage, 'package_name', '/local/package.tar.gz')

    def testListPackage(self):
        listPackageCmd = 'pm_run_exe -pm nuwa_address GetPackageList'
        data = 'reply: [0]\nerror: [NA]\n\t\tpackage://package_name\n'
        error = ""
        code = 0
        resultDict = {listPackageCmd : (data, error, code)}
        self.pmRun.process = fake_process.FakeProcess(resultDict)
        data, error, code = self.pmRun.listPackage()
        self.assertTrue(code == 0)

if __name__ == "__main__":
    unittest.main()
