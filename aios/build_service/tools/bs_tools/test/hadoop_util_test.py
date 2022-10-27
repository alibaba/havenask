#!/bin/env /usr/bin/python

import sys
import os
curPath = os.path.split(os.path.realpath(__file__))[0]
parentPath = curPath + "/../"
sys.path.append(parentPath)

import hadoop_util
import unittest

class hadoop_util_test(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testRunPipesFailure(self):
        hadoopUtil = hadoop_util.HadoopUtil("/hadoop/home")
        hasException = False
        try:
            hadoopUtil.runPipes("", "", "", "", "", "", "", "")
        except:
            hasException = True
        self.assertTrue(hasException)
