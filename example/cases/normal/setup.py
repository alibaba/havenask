# *-* coding:utf-8 *-*
import os
import sys
HERE = os.path.split(os.path.realpath(__file__))[0]


def build_prepare():
    configRoot = os.path.join(HERE, './config/offline_config/')
    dataPath = os.path.join(HERE, '../../data/test.data')
    return configRoot, dataPath
        
        
def search_prepare():
    configDir = os.path.join(HERE, './config/online_config')
    bizsConfigDir = os.path.join(configDir, "bizs")
    testQueries = [
        "select id,hits from in0 where MATCHINDEX(\'title\', \'搜索词典\')"
    ]
    return configDir, testQueries
