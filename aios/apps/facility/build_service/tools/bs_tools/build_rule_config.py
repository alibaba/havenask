# -*- coding: utf-8 -*-

import os
import process
import zipfile
import include.json_wrapper as json
from common_define import *


class BuildRuleConfig(object):
    def __init__(self, fsUtil, clusterName, configPath):
        clusterZipPath = os.path.join(configPath, BUILD_CLUSTERS_ZIP_FILE_NAME)
        if os.path.exists(clusterZipPath):
            z = zipfile.ZipFile(clusterZipPath, 'r')
            clusterFileName = BUILD_CLUSTERS_DIR_NAME + '/' + clusterName + BUILD_CLUSTER_FILE_SUFFIX
            context = z.read(clusterFileName)
        else:
            context = fsUtil.cat(os.path.join(configPath,
                                              BUILD_CLUSTERS_DIR_NAME + '/' + clusterName + BUILD_CLUSTER_FILE_SUFFIX))
        jsonMap = json.read(context)
        if jsonMap.has_key('cluster_config') and jsonMap['cluster_config'].has_key('builder_rule_config'):
            jsonMap = jsonMap['cluster_config']['builder_rule_config']
        else:
            jsonMap = {}
        self.partitionCount = self.__getValueWithDefault(jsonMap, BUILD_RULE_PARTITION_COUNT, 1)
        self.partitionSplitNum = self.__getValueWithDefault(jsonMap, BUILD_RULE_PARTITION_SPLIT_NUM, 1)
        self.buildParallelNum = self.__getValueWithDefault(jsonMap, BUILD_RULE_BUILD_PARALLEL_NUM, 1)
        self.mergeParallelNum = self.__getValueWithDefault(jsonMap, BUILD_RULE_MERGE_PARALLEL_NUM, 1)
        self.mapReduceRatio = self.__getValueWithDefault(jsonMap, BUILD_RULE_MAP_REDUCE_RATIO, 1)
        self.needPartition = self.__getValueWithDefault(jsonMap, BUILD_RULE_NEED_PARTITION, True)

    def __getValueWithDefault(self, jsonMap, name, defaultValue=None):
        if jsonMap.has_key(name):
            return jsonMap[name]
        return defaultValue
