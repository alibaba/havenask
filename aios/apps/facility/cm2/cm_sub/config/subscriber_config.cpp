/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "aios/apps/facility/cm2/cm_sub/config/subscriber_config.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include "aios/apps/facility/cm2/cm_basic/common/zk_option.h"

namespace cm_sub {

using namespace cm_basic;

AUTIL_LOG_SETUP(cm_sub, SubscriberConfig);

SubscriberConfig::SubscriberConfig()
    : _serverType(FromZK)
    , _subMaster(false)
    , _timeout(10)
    , _subType(SubReqMsg::ST_PART)
    , _compressType(CT_NONE)
    , _isLocalMode(false)
    , _readCache(false)
    , _writeCache(false)
    , _writeInterval(5 * 60)
    , _conhashWeightRatio(1)
    , _virtualNodeRatio(0)
    , _disableTopo(false)
{
    memset(_zkServer, 0, MAX_LEN);
    memset(_zkPath, 0, MAX_LEN);
    memset(_cmServer, 0, MAX_LEN);
    memset(_clusterCfg, 0, PATH_MAX);

    _cacheFile[0] = '\0';
}

SubscriberConfig::~SubscriberConfig() {}

int32_t SubscriberConfig::init(const char* config_path)
{
    // mxml node
    mxml_node_t* keynode = NULL;
    mxml_node_t* topnode = NULL;

    // open xml file and init topnode
    FILE* fp = fopen(config_path, "r");
    if (fp == NULL) {
        AUTIL_LOG(ERROR, "hb_node : SubscriberConfig::init(), fopen config_file : %s failed !!", config_path);
        return -1;
    }

    topnode = mxmlLoadFile(NULL, fp, MXML_NO_CALLBACK); // 根 ？xml节点
    fclose(fp);
    if (topnode == NULL) {
        AUTIL_LOG(ERROR, "subscriber config init failed, load xml file failed. file: %s", config_path);
        return -1;
    }

    keynode = mxmlFindElement(topnode, topnode, "cm_server", NULL, NULL, MXML_DESCEND);
    if (keynode == NULL || parseCMServer(keynode) != 0) {
        mxmlDelete(topnode);
        return -1;
    }

    keynode = mxmlFindElement(topnode, topnode, "cm_subscriber", NULL, NULL, MXML_DESCEND);
    if (keynode != NULL) {
        if (parseSubscriber(keynode) != 0) {
            mxmlDelete(topnode);
            return -1;
        }
    }

    mxmlDelete(topnode);
    return 0;
}

/* ============= parse cm_server to init cm_server =============*/
int32_t SubscriberConfig::parseCMServer(mxml_node_t* keynode)
{
    mxml_node_t* tmpnode = NULL;

    tmpnode = mxmlFindElement(keynode, keynode, "zk_mode", NULL, NULL, MXML_DESCEND_FIRST);
    const char* zk_server = mxmlElementGetAttr(tmpnode, "value");
    const char* zk_path = mxmlElementGetAttr(tmpnode, "zk_path");
    if (zk_server && zk_path) {
        _serverType = FromZK;
        snprintf(_zkServer, MAX_LEN, "%s", zk_server);
        snprintf(_zkPath, MAX_LEN, "%s", zk_path);
        const char* timeout = mxmlElementGetAttr(tmpnode, "timeout");
        if (timeout) {
            _timeout = strtoul(timeout, NULL, 10);
        }
        const char* sub_master = mxmlElementGetAttr(tmpnode, "sub_master");
        if (sub_master && strncasecmp(sub_master, "yes", 3) == 0) {
            _subMaster = true;
        }
        AUTIL_LOG(INFO, "hb_node: zk_server = %s, zk_path = %s, sub_master = %s", _zkServer, _zkPath,
                  _subMaster ? "yes" : "no");
    } else {
        // init _masterIp, _tcpPort, _udpPort
        tmpnode = mxmlFindElement(keynode, keynode, "cm_mode", NULL, NULL, MXML_DESCEND_FIRST);
        const char* cm_server = mxmlElementGetAttr(tmpnode, "value");
        if (cm_server) {
            _serverType = FromCfg;
            snprintf(_cmServer, MAX_LEN, "%s", cm_server);
        } else {
            return -1;
        }
    }
    return 0;
}

/* ============= parse cm_subscriber to init cm_sub =============*/
int32_t SubscriberConfig::parseSubscriber(mxml_node_t* keynode)
{
    mxml_node_t* tmpnode = NULL;

    // init sub_type
    tmpnode = mxmlFindElement(keynode, keynode, "sub_type", NULL, NULL, MXML_DESCEND_FIRST);
    const char* disable_topo = mxmlElementGetAttr(tmpnode, "_test_disable_topo");
    if (disable_topo && strncasecmp(disable_topo, "yes", 3) == 0) {
        _disableTopo = true;
        AUTIL_LOG(WARN, "cm_sub: disable topo cluster building, config only used for TEST");
    }
    const char* stype = mxmlElementGetAttr(tmpnode, "value");
    if (stype && strncasecmp(stype, "st_all", 6) == 0) {
        _subType = SubReqMsg::ST_ALL;
        AUTIL_LOG(INFO, "cm_sub : SubscriberConfig::init(), sub_type = st_all");
    } else if (strcasecmp(stype, "st_belong") == 0) {
        _subType = SubReqMsg::ST_BELONG;
        AUTIL_LOG(INFO, "cm_sub : SubscriberConfig::init(), sub_type = st_belong");
        tmpnode = mxmlFindElement(keynode, keynode, "belong_list", NULL, NULL, MXML_DESCEND_FIRST);
        tmpnode = mxmlFindElement(tmpnode, keynode, "belong", NULL, NULL, MXML_DESCEND_FIRST);
        while (tmpnode) {
            const char* name = mxmlElementGetAttr(tmpnode, "name");
            if (name) {
                _subBelongSet.insert(name);
            }
            tmpnode = mxmlFindElement(tmpnode, keynode, "belong", NULL, NULL, MXML_NO_DESCEND);
        }
        if (_subBelongSet.size() == 0) {
            AUTIL_LOG(ERROR, "cm_sub : SubscriberConfig::init(), belong_list is null !!");
            return -1;
        }
    } else {
        _subType = SubReqMsg::ST_PART;
        AUTIL_LOG(INFO, "cm_sub : SubscriberConfig::init(), sub_type = st_part");

        tmpnode = mxmlFindElement(keynode, keynode, "cluster_list", NULL, NULL, MXML_DESCEND_FIRST);
        tmpnode = mxmlFindElement(tmpnode, keynode, "cluster", NULL, NULL, MXML_DESCEND_FIRST);
        while (tmpnode) {
            const char* name = mxmlElementGetAttr(tmpnode, "name");
            if (name) {
                _subClusterSet.insert(name);
            }
            tmpnode = mxmlFindElement(tmpnode, keynode, "cluster", NULL, NULL, MXML_NO_DESCEND);
        }
        if (_subClusterSet.size() == 0) {
            AUTIL_LOG(ERROR, "cm_sub : SubscriberConfig::init(), cluster_list is null !!");
            return -1;
        }
    }
    //
    _compressType = CT_NONE;
    tmpnode = mxmlFindElement(keynode, keynode, "compress_type", NULL, NULL, MXML_DESCEND_FIRST);
    const char* ctype = mxmlElementGetAttr(tmpnode, "value");
    if (ctype && strncasecmp(ctype, "ct_snappy", 9) == 0) {
        _compressType = CT_SNAPPY;
        AUTIL_LOG(INFO, "cm_sub : SubscriberConfig::init(), compress_type = ct_snappy");
    }

    tmpnode = mxmlFindElement(keynode, keynode, "local_mode", NULL, NULL, MXML_DESCEND_FIRST);
    const char* is_local = mxmlElementGetAttr(tmpnode, "enable");
    if (is_local && strncasecmp(is_local, "yes", 3) == 0) {
        const char* cluster_config = mxmlElementGetAttr(tmpnode, "cluster_config");
        if (cluster_config) {
            _isLocalMode = true;
            snprintf(_clusterCfg, PATH_MAX, "%s", cluster_config);
        }
        AUTIL_LOG(INFO, "cm_sub : SubscriberConfig::init(), local_mode: cluster_config = %s", _clusterCfg);
    }
    tmpnode = mxmlFindElement(keynode, keynode, "policy", NULL, NULL, MXML_DESCEND_FIRST);
    const char* conhash = mxmlElementGetAttr(tmpnode, "weight_ratio");
    if (conhash) {
        _conhashWeightRatio = (_conhashWeightRatio = atoi(conhash)) <= 0 ? 1 : _conhashWeightRatio;
    }
    AUTIL_LOG(INFO, "cm_sub : conhash weight_ratio %d", _conhashWeightRatio);
    const char* virtual_node_ratio = mxmlElementGetAttr(tmpnode, "virtual_node_ratio");
    if (virtual_node_ratio) {
        _virtualNodeRatio = (_virtualNodeRatio = atoi(virtual_node_ratio)) <= 0 ? 0 : _virtualNodeRatio;
    }
    AUTIL_LOG(INFO, "cm_sub : conhash virtual_node_ratio %d", _virtualNodeRatio);
    parseCache(keynode);
    return 0;
}

void SubscriberConfig::parseCache(mxml_node_t* keynode)
{
    mxml_node_t* tmpnode = NULL;
    tmpnode = mxmlFindElement(keynode, keynode, "local_cache", NULL, NULL, MXML_DESCEND_FIRST);
    const char* is_write = mxmlElementGetAttr(tmpnode, "enable_write");
    if (is_write && strncasecmp(is_write, "yes", 3) == 0) {
        _writeCache = true;
    }
    const char* is_read = mxmlElementGetAttr(tmpnode, "enable_read");
    if (is_read && strncasecmp(is_read, "yes", 3) == 0) {
        _readCache = true;
    }
    const char* interval_s = mxmlElementGetAttr(tmpnode, "interval");
    if (interval_s != NULL) {
        _writeInterval = (_writeInterval = atoi(interval_s)) <= 0 ? 5 * 60 : _writeInterval;
    }
    const char* cache_file = mxmlElementGetAttr(tmpnode, "file");
    cache_file ? strcpy(_cacheFile, cache_file) : strcpy(_cacheFile, "./local_cache/cluster_cache.xml");
    AUTIL_LOG(INFO, "cm_sub : local cache: write: %s, read: %s, file: %s", _writeCache ? "yes" : "no",
              _readCache ? "yes" : "no", _cacheFile);
}

} // namespace cm_sub
