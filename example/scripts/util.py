#!/bin/env python
import os
import sys

HERE = os.path.split(os.path.realpath(__file__))[0]

def getConfigMaxVersion(configRoot):
    dirList = os.listdir(configRoot)
    versions = []
    for d in dirList:
        try:
            v = int(d)
            versions.append(v)
        except:
            pass
    versions.sort()
    return versions

def copyAithetaIndexerPlugin(configRoot):
    versions = getConfigMaxVersion(configRoot)
    if len(versions) <= 0:
        print 'config %s is invalid, a num sub dir must be exist' % configRoot
        return False

    soTarget = '%s/plugins/libaitheta_indexer.so' % (os.path.join(configRoot, str(versions[-1])))
    if os.path.exists(soTarget):
        return True

    soPath = os.path.join(HERE, "../../aios/plugin_platform/indexer_plugins/aitheta_indexer/lib/libaitheta_indexer.so")
    if not os.path.exists(soPath):
        soPath = '/ha3_depends/usr/local/lib64/libaitheta_indexer.so'
    if not os.path.exists(soPath):
        print 'please compile aitheta_indexer plugin first'
        return False;

    dirList = os.listdir(configRoot)
    cmd = 'cp %s %s/plugins/' % (soPath, os.path.join(configRoot, str(versions[-1])))
    print cmd
    os.system(cmd)
    return True
