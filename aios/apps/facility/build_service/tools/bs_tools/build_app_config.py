#!/bin/env python

import os
import process
import include.json as json
from common_define import *

class BuildAppConfig(object):
    def __init__(self, fsUtil=None, configPath=None):
        if fsUtil is None and configPath is None:
            # for test
            return
        buildAppFile = os.path.join(configPath, BUILD_APP_CONFIG_FILE_NAME)
        if fsUtil.exists(buildAppFile):
            jsonMap = json.read(fsUtil.cat(buildAppFile))
        else:
            jsonMap = {}

        self.userName = self.__getValueWithDefault(jsonMap, BUILD_APP_USER_NAME, '')
        self.serviceName = self.__getValueWithDefault(jsonMap, BUILD_APP_SERVICE_NAME, '')
        self.nuwaAddress = self.__getValueWithDefault(jsonMap, BUILD_APP_NUWA_ADDRESS)
        self.apsaraPackageName = self.__getValueWithDefault(jsonMap, BUILD_APP_APSARA_PACKAGE_NAME)
        self.hadoopPackageName = self.__getValueWithDefault(jsonMap, BUILD_APP_HADOOP_PACKAGE_NAME)

        self.indexRoot = self.__getValueWithDefault(jsonMap, BUILD_APP_INDEX_ROOT)
        self.zookeeperRoot = self.__getValueWithDefault(jsonMap, BUILD_APP_ZOOKEEPER_ROOT, '')
        self.hippoRoot = self.__getValueWithDefault(jsonMap, BUILD_APP_HIPPO_ROOT)
        self.amonitorPort = self.__getValueWithDefault(jsonMap, BUILD_APP_AMONITOR_PORT, '10086')

    def __getValueWithDefault(self, jsonMap, name, defaultValue = None):
        if jsonMap.has_key(name):
            return jsonMap[name]
        return defaultValue
