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

def copyPlugin(candidateSourcePaths = [
    os.path.join(HERE, "../../aios/plugin_platform/indexer_plugins/aitheta_indexer/lib/"),
    '/ha3_depends/usr/local/lib64/',
    '/ha3_install/usr/local/lib64',
    '../ha3_install/usr/local/lib64',
    os.path.join(os.path.expanduser("~"), 'havenask/ha3_install/usr/local/lib64')
    ],
               plugin = "",
               targetPath = ""):
    versions = getConfigMaxVersion(targetPath)
    if len(versions) <= 0:
        print 'config %s is invalid, a num sub dir must be exist' % targetPath
        return False
    soTarget = '%s/plugins/%s' % (os.path.join(targetPath, str(versions[-1])), plugin)

    for path in candidateSourcePaths:
        soPath = os.path.join(os.path.abspath(path), plugin)
        if os.path.exists(soPath):
            dirList = os.listdir(targetPath)
            cmd = 'cp %s %s/plugins/' % (soPath, os.path.join(targetPath, str(versions[-1])))
            print cmd
            os.system(cmd)
            return True
    print 'plugin %s not found' %(plugin)
    return False
