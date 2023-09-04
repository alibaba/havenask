#!/bin/env python

import os
import time
import json

from hippo_py_sdk.include import zk_connector
from hippo_py_sdk.log import *

def get_leader(leader_path, timeout=10, verbose=False):
    zk_server, sub_path = zk_connector.parse_zk_path(leader_path)
    my_zk_connector = zk_connector.getZkConnector(zk_server, timeout)
    if my_zk_connector is None:
        if verbose:
            log_error('connect to zookeeper[%s] failed.' % zk_server)
        return None
    elector_nodes = get_leader_elector_nodes(
        my_zk_connector, sub_path, timeout, verbose)
    if elector_nodes is None:
        my_zk_connector.close()
        return None
    if not elector_nodes:
        if verbose:
            log_error('Can not find any leader on %s.' % leader_path)
        return None
    elector_nodes.sort()
    leader_node = elector_nodes[0]
    ret, leader_address = my_zk_connector.get(os.path.join(
            sub_path, leader_node))
    if not ret:
        if verbose:
            log_error('read leader node failed.')
        my_zk_connector.close()
        return None
    my_zk_connector.close()
    return leader_address

def get_leader_http(leader_path, timeout=10, verbose=False):
    zk_server, sub_path = zk_connector.parse_zk_path(leader_path)
    my_zk_connector = zk_connector.getZkConnector(zk_server, timeout)
    if my_zk_connector is None:
        if verbose:
            log_error('connect to zookeeper[%s] failed.' % zk_server)
        return None
    ret, content = my_zk_connector.get(sub_path)
    if not ret:
        if verbose:
            log_error('read leader info failed')
        return None
    try:
        json_obj = json.loads(content)
        return json_obj['httpAddress']
    except:
        log_error('parse leader info failed [%s]' % content)
        return None

def get_leader_elector_nodes(my_zk_connector, leader_elector_path,
                             timeout, verbose):
    error_msg = ''
    while timeout > 0:
        try:
            ret, elector_nodes = my_zk_connector.lsdir(leader_elector_path)
        except Exception, e:
            error_msg = ('list leader elector dir failed, '
                         'leader_path = %s' % leader_elector_path)
            timeout = timeout - 1
            time.sleep(1)
            continue
        if not ret:
            error_msg =  'no leader elector node'
            break
        return elector_nodes
    if verbose:
        log_error(error_msg)
    return None

if __name__ == '__main__':
    zk = zk_connector.getZkConnector('10.125.224.30:2181', 10)
    zk.set('/yangkt/test_hippo_admin/admin_leader/leader_elector/1', '1.1.1.1:222')
    print get_leader('zfs://10.125.224.30:2181/yangkt/test_hippo_admin/admin_leader/leader_elector')
    print get_leader('10.125.224.30:2181/yangkt/test_hippo_admin/admin_leader/leader_elector')    
    print get_leader_http('zfs://10.125.224.30:2181/yangkt/test_hippo_admin/admin_leader/leader_elector')
    zk.set('/yangkt/test_hippo_admin/admin_leader/leader_elector', '''{
    "httpPort" : 1234,
    "httpAddress" : "1.1.1.1:4321"
}
''')
    print get_leader_http('zfs://10.125.224.30:2181/yangkt/test_hippo_admin/admin_leader/leader_elector')
