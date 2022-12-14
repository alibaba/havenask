# *-* coding:utf-8 *-*
import os
import sys
HERE = os.path.split(os.path.realpath(__file__))[0]
sys.path.append(os.path.abspath(os.path.join(HERE, "../../common")))
import util


def build_prepare():
    configRoot = os.path.join(HERE, './config/offline_config/')
    dataPath = os.path.join(HERE, '../../data/test.data')
    if not util.copyPlugin(plugin="libanalyzer_plugin.so", targetPath=configRoot):
        raise IOError
    return configRoot, dataPath
        
        
def search_prepare():
    configDir = os.path.join(HERE, './config/online_config')
    bizsConfigDir = os.path.join(configDir, "bizs")
    if not util.copyPlugin(plugin="libanalyzer_plugin.so", targetPath=bizsConfigDir):
        raise IOError
    testQueries = [
        "select id,hits from in0 where MATCHINDEX(\'title\', \'搜索词典\')&&kvpair=timeout:10000"
    ]
    return configDir, testQueries
