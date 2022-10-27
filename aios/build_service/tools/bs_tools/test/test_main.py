#!/bin/env python
import re
import os
import sys
import types
import unittest

def addTest(testSuite, moduleName):
    testModule = __import__(moduleName)
    for name in dir(testModule):
        obj = getattr(testModule, name)
        if type(obj) == types.TypeType and issubclass(obj, unittest.TestCase):
            print 'run case: %s' % str(obj)
            testSuite.addTest(unittest.makeSuite(obj, 'test'))

def runTest():
    runner = unittest.TextTestRunner()        
    allTests = unittest.TestSuite()

    curPath = os.path.split(os.path.realpath(__file__))[0]
    caseList = os.listdir(curPath)
    for case in caseList:
        m = re.match(r"(.*?)_test.py$", case)
        if m is None:
            continue
        caseName = m.group(1) + "_test"
        addTest(allTests, caseName)

    return runner.run(allTests).wasSuccessful()

if __name__ == "__main__":
    runTest()
