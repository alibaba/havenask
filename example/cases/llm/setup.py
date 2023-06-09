# *-* coding:utf-8 *-*
import os
import sys
HERE = os.path.split(os.path.realpath(__file__))[0]
sys.path.append(os.path.abspath(os.path.join(HERE, "../../common")))
import util


def build_prepare():
    configRoot = os.path.join(HERE, './config/offline_config/')
    dataPath = os.path.join(HERE, '../../data/llm.data')
    if not util.copyPlugin(plugin="libaitheta_indexer.so", targetPath=configRoot):
        sys.exit(-1)
    return configRoot, dataPath


def search_prepare():
    configDir = os.path.join(HERE, './config/online_config')
    tableConfigDir = os.path.join(configDir, "table")
    if not util.copyPlugin(plugin="libaitheta_indexer.so", targetPath=tableConfigDir):
        sys.exit(-1)
    testQueries = [
    ]
    return configDir, testQueries
