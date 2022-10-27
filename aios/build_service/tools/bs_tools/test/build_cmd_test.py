# -*- coding: utf-8 -*-

import os
import sys
import time
import shutil
import unittest
from mock import *
from tools_test_common import *

from build_service.proto.Admin_pb2 import StartBuildRequest
from build_service.proto.Admin_pb2 import StartBuildResponse
from build_service.proto.Admin_pb2 import StopBuildRequest

import json
from build_cmd import StartBuildCmd
from build_cmd import StopBuildCmd
from build_cmd import StepBuildCmd
from build_cmd import CreateVersionCmd
from build_cmd import SwitchCmd

class Options:
    pass

class StartBuildCmdTest(unittest.TestCase):
    def setUp(self):
        self.workDir = TEST_DATA_TEMP_PATH + "build_cmd_test/"
        if os.path.exists(self.workDir):
            shutil.rmtree(self.workDir)
        os.mkdir(self.workDir)

    def writeTempFile(self, name, content):
        f = open(self.workDir + '/' + name, 'w')
        f.write(content)
        f.close()
        return self.workDir + '/' + name

    def testSimple(self):
        cmd = StartBuildCmd()
        callMock = MagicMock()
        setattr(cmd, 'callWithTimeout', callMock)

        configPath = TEST_DATA_PATH + '/start_build_cmd_test/config'

        getConfigMock = MagicMock()
        getConfigMock.return_value = configPath
        setattr(cmd, 'getLatestConfig', getConfigMock)

        getAdminMock = MagicMock()
        getAdminMock.return_value = 'admin'
        setattr(cmd, 'getAdminAddress', getAdminMock)

        request = StartBuildRequest()
        request.configPath = configPath
        request.buildId.appName = '_'
        request.buildId.generationId = 1
        request.buildId.dataTable = 'simple1'
        request.dataDescriptionKvs = '[]'
        request.buildMode = 'full'
        request.debugMode = 'step'
        request.jobMode = True
        cmd.parseParams(['-c', configPath, '-g', '1', '-d', 'simple1', '--debug_mode', 'step', '--job'])
        callMock.return_value = StartBuildResponse()
        cmd.run()
        callMock.assert_called_once_with(method=cmd.ADMIN_START_BUILD_METHOD,
                                         request=request, adminAddr='admin',
                                         timeout=cmd.timeout)

    def testBuildFromInc(self):
        configPath = TEST_DATA_PATH + '/start_build_cmd_test/config'
        dstConfigPath = '/'.join([self.workDir, 'config'])
        shutil.copytree(configPath, dstConfigPath)
        self.writeTempFile('config/build_app.json',
                           '{"index_root" : "%s"}' % self.workDir)

        cmd = StartBuildCmd()

        getConfigMock = MagicMock()
        getConfigMock.return_value = dstConfigPath
        setattr(cmd, 'getLatestConfig', getConfigMock)

        getAdminMock = MagicMock()
        getAdminMock.return_value = 'admin'
        setattr(cmd, 'getAdminAddress', getAdminMock)

        callMock = MagicMock()
        setattr(cmd, 'callWithTimeout', callMock)


        request = StartBuildRequest()
        request.configPath = dstConfigPath
        request.buildId.appName = 'app'
        request.buildId.generationId = 1
        request.buildId.dataTable = 'simple1'
        request.dataDescriptionKvs = '[]'
        request.buildMode = 'incremental'
        request.debugMode = 'run'
        cmd.parseParams(['-c', dstConfigPath,'-a','app', '-g', '1', '-d', 'simple1', '-m', 'incremental'])
        callMock.return_value = StartBuildResponse()
        cmd.run()
        callMock.assert_called_once_with(method=cmd.ADMIN_START_BUILD_METHOD,
                                         request=request, adminAddr='admin',
                                         timeout=cmd.timeout)

        # failed
        clusterIndexRoot = '/'.join([self.workDir, 'cluster1', 'generation_1'])
        os.makedirs(clusterIndexRoot)
        self.assertRaises(Exception, cmd.run)

        os.mkdir(clusterIndexRoot + '/partition_0_65535')
        self.assertRaises(Exception, cmd.run)
        shutil.rmtree(clusterIndexRoot + '/partition_0_65535')

        os.mkdir(clusterIndexRoot + '/partition_0_32767')
        os.mkdir(clusterIndexRoot + '/partition_32768_65535')
        self.assertRaises(Exception, cmd.run)

        self.writeTempFile('cluster1/generation_1/partition_0_32767/schema.json', '')
        self.assertRaises(Exception, cmd.run)

        self.writeTempFile('cluster1/generation_1/partition_32768_65535/schema.json', '')
        cmd.run()

    def testCheckOptionsValidity(self):
        cmd = StartBuildCmd()
        options = Options()
        options.configPath = 'config'
        options.generationId = None
        options.dataTable = None
        options.dataDescriptionKv = None
        options.dataDescriptionFile = None
        self.assertRaises(Exception, cmd.checkOptionsValidity, options)
        options.dataTable = 'dataTable'
        cmd.checkOptionsValidity(options)
        options.generationId = -2
        self.assertRaises(Exception, cmd.checkOptionsValidity, options)
        options.generationId = 1

        options.dataDescriptionKv = 'kv'
        cmd.checkOptionsValidity(options)
        options.dataDescriptionFile = 'file'
        self.assertRaises(Exception, cmd.checkOptionsValidity, options)
        options.dataDescriptionKv = None
        cmd.checkOptionsValidity(options)

    def testTransformDataDesc(self):
        cmd = StartBuildCmd()
        configPath = TEST_DATA_PATH + '/start_build_cmd_test/config'
        self.assertRaises(Exception, cmd._transformDataDesc, "file", None)
        self.assertEqual([{'src':''}], json.loads(cmd._transformDataDesc(
                    configPath, 'simple1', "src=&&", None)))
        self.assertEqual([{'src':'file'}], json.loads(cmd._transformDataDesc(
                    configPath, 'simple1', "src=file&&;", None)))
        self.assertEqual([{'data':'a.data','document_format':'ha3'}, {'topic_name':'=topic'}],
                         json.loads(cmd._transformDataDesc(
                    configPath, 'simple1', "data=a.data&&document_format=ha3;topic_name==topic")))
        self.assertRaises(Exception, cmd._transformDataDesc, None, 'not exist')
        file = self.writeTempFile('invalid.json', 'a')
        self.assertRaises(Exception, cmd._transformDataDesc, None, file)
        file = self.writeTempFile('valid.json', json.dumps([{'data':'a.dat','document_format':'ha3'},{'topic_name':'=topic'}]))
        self.assertEqual([{'data':'a.dat','document_format':'ha3'},{'topic_name':'=topic'}],
                         json.loads(cmd._transformDataDesc(
                    configPath, 'simple1', None, file)))
        self.assertEqual([], json.loads(cmd._transformDataDesc(
                    configPath, 'simple1', None, None)))

        # test merge with config
        ## cmd line is empty
        self.assertEqual([{'data' : 'hdfs://data', 'src' : 'file'},
                          {'src' : 'swift', 'swift_root' : 'zfs://root', 'topic_name':'raw_doc'},
                          {'src' : 'plugin', 'type' : 'plugin'}],
                         json.loads(cmd._transformDataDesc(configPath, 'simple2', None, None)))

        self.assertEqual([{'data' : 'hdfs://data', 'src' : 'file', 'format':'isearch'},
                          {'src' : 'swift', 'swift_root' : 'zfs://root', 'topic_name':'aaa'},
                          {'src' : 'plugin', 'type' : 'plugin'}],
                         json.loads(cmd._transformDataDesc(
                    configPath, 'simple2',
                    'format=isearch&&src=file;src=swift&&topic_name=aaa', None)))

    def testMergeDataDescription(self):
        cmd = StartBuildCmd()
        desc1 = [{}, {'key1':'value1', 'key2':'value2'}]
        desc2 = [{'key3':'value3','key4':'value4'},{'key1':'old_value1'},{'key5':'value5'}]
        desc3 = cmd._mergeDataDescriptions(desc1, desc2)
        self.assertEqual([{'key3':'value3','key4':'value4'},
                          {'key1':'value1','key2':'value2'},
                          {'key5':'value5'}],
                         desc3)

        desc1 = [{}, {'key1':'value1', 'key2':'value2'}]
        desc2 = [{'key3':'value3','key4':'value4'}]
        desc3 = cmd._mergeDataDescriptions(desc1, desc2)
        self.assertEqual([{'key3':'value3','key4':'value4'},
                          {'key1':'value1','key2':'value2'}],
                         desc3)

    def testCopyGenerationMetas(self):
        # prepare config
        self.writeTempFile('build_app.json', '{"index_root" : "%s"}' % self.workDir)
        os.mkdir(os.path.join(self.workDir, 'data_tables'))
        content = '''
{
              "processor_chain_config" : [
                  {
                      "clusters" : [
                          "cluster1",
                          "cluster2"
                      ]
                  },
                  {
                      "clusters" : [
                          "cluster3"
                      ]
                  }
              ]
}
                  '''
        self.writeTempFile('data_tables/simple_table.json', content)

        # prepare generation_meta
        gm = self.writeTempFile('generation_meta', '')

        cmd = StartBuildCmd()
        cmd.copyGenerationMeta(self.workDir, 'simple', 0, gm)

        self.assertTrue(os.path.exists(self.workDir + '/cluster1/generation_0/generation_meta'))
        self.assertTrue(os.path.exists(self.workDir + '/cluster2/generation_0/generation_meta'))
        self.assertTrue(os.path.exists(self.workDir + '/cluster3/generation_0/generation_meta'))

class StopBuildCmdTest(unittest.TestCase):
    def testCheckOptionsValidity(self):
        cmd = StopBuildCmd()
        options = Options()
        options.configPath = 'config'
        options.generationId = None
        options.dataTable = None
        cmd.checkOptionsValidity(options)

        options.generationId = 1
        self.assertRaises(Exception, cmd.checkOptionsValidity, options)

        options.dataTable = 'dataTable'
        options.generationId = -2
        self.assertRaises(Exception, cmd.checkOptionsValidity, options)

        options.generationId = 1
        cmd.checkOptionsValidity(options)

class CreateVersionCmdTest(unittest.TestCase):
    def testCheckOptionsValidity(self):
        cmd = CreateVersionCmd()
        options = Options()
        options.configPath = 'config'
        options.generationId = None
        options.dataTable = None
        options.clusterName = None
        self.assertRaises(Exception, cmd.checkOptionsValidity, options)
        options.dataTable = 'dataTable'
        self.assertRaises(Exception, cmd.checkOptionsValidity, options)
        options.generationId = 1
        self.assertRaises(Exception, cmd.checkOptionsValidity, options)
        options.clusterName = 'cluster'
        cmd.checkOptionsValidity(options)
        options.mergeConfigName = 'merge'
        cmd.checkOptionsValidity(options)

class SwitchCmdTest(unittest.TestCase):
    def testCheckOptionsValidity(self):
        cmd = SwitchCmd()
        options = Options()
        options.configPath = 'config'
        options.generationId = None
        options.dataTable = None
        self.assertRaises(Exception, cmd.checkOptionsValidity, options)

        options.dataTable = 'dataTable'
        self.assertRaises(Exception, cmd.checkOptionsValidity, options)

        options.generationId = 1
        cmd.checkOptionsValidity(options)

class StepBuildCmdTest(unittest.TestCase):
    def testCheckOptionsValidity(self):
        cmd = StepBuildCmd()
        options = Options()
        options.configPath = 'config'
        options.generationId = None
        options.dataTable = None
        self.assertRaises(Exception, cmd.checkOptionsValidity, options)

        options.dataTable = 'dataTable'
        self.assertRaises(Exception, cmd.checkOptionsValidity, options)

        options.generationId = 1
        cmd.checkOptionsValidity(options)
