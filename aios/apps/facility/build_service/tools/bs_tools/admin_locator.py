# -*- coding: utf-8 -*-

import os
import include.json_wrapper as json
from hippo_py_sdk import leader_locator

HIPPOROOT_KEY = 'hippo_root'
ZOOKEEPER_KEY = 'zookeeper_root'
VULCAN_AM_LEADER_ELECTION_BASE_NAME = "app_master/LeaderElection"
BS_AM_LEADER_ELECTION_BASE_NAME = "admin/LeaderElection"


def getAdminAddress(configPath, appMode, fsUtil, timeout=5):
    zkRoot = getBuildAppConfig(configPath, ZOOKEEPER_KEY, fsUtil)
    if not zkRoot:
        raise Exception('get zookeeper addr from [%s] build_app.json failed' % configPath)
    return getAdminAddressFromZk(zkRoot, appMode, timeout)


def getAdminAddressFromZk(zkRoot, appMode, timeout=5):
    leaderElectorName = BS_AM_LEADER_ELECTION_BASE_NAME
    if (appMode == "VULCAN_APP"):
        leaderElectorName = VULCAN_AM_LEADER_ELECTION_BASE_NAME
    leaderElectorPath = os.path.join(zkRoot, leaderElectorName)
    leaderAddress = leader_locator.get_leader(leaderElectorPath, timeout, verbose=True)
    if not leaderAddress:
        return None
    try:
        leaderAddress = json.read(leaderAddress)
    except Exception as e:
        return "tcp:" + leaderAddress
    if 'address' in leaderAddress:
        return "tcp:" + leaderAddress['address']
    else:
        raise Exception('no address found in leader info %s' % str(leaderAddress))


def getBuildAppConfig(configPath, key, fsUtil):
    buildAppFilePath = os.path.join(configPath, 'build_app.json')
    if not fsUtil.exists(buildAppFilePath):
        raise Exception('config[%s] build_app.json not exist' % configPath)
    content = fsUtil.cat(buildAppFilePath)
    if not content:
        raise Exception('Read config[%s] failed.' % configPath)

    config = json.read(content)
    if not config.has_key(key):
        raise Exception('[%s] not exist in build_app.json of [%s]' % (key, configPath))

    return config[key]
