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
/**
 * =====================================================================================
 *
 *       Filename:  subscriber_config.h
 *
 *    Description:  cm_subscriber.xml 配置文件解析
 *
 *        Version:  0.1
 *        Created:  2012-08-07 15:11:09
 *       Revision:  none
 *       Compiler:  glibc
 *
 *         Author:  tiechou (search engine group), tiechou@taobao.com
 *        Company:  www.taobao.com
 *
 * =====================================================================================
 */
#ifndef __SUBSCRIBER_CONFIG_H_
#define __SUBSCRIBER_CONFIG_H_

#include <set>

#include "aios/apps/facility/cm2/cm_basic/basic_struct/proto/cm_node.pb.h"
#include "aios/apps/facility/cm2/cm_basic/basic_struct/proto/cm_subscriber.pb.h"
#include "autil/Log.h"
#include "mxml.h"

namespace cm_sub {

enum ServerType { FromZK = 0, FromCfg = 1 };

class SubscriberConfig
{
public:
    enum { MAX_LEN = 1024 }; ///<
    /* ====== cm_server ======= */
    ServerType _serverType;
    std::string _zkServer;
    std::string _zkPath;
    bool _subMaster;       // 仅从master订阅，默认false
    uint32_t _timeout;     ///< zookeeper超时时间，单位(s)
    std::string _cmServer; ///< 只有当_serverType=FromCfg时才会用

    /* ====== cm_subscriber ======= */
    cm_basic::SubReqMsg::SubType _subType;
    cm_basic::CompressType _compressType;
    std::set<std::string> _subClusterSet; ///< 只有当_subType=ST_PART时才会用到
    std::set<std::string> _subBelongSet;  ///< ST_BELONG 时使用
    bool _isLocalMode;                    ///< 使用本地模式
    std::string _clusterCfg;              ///< 订阅集群的本地配置文件
    std::string _cacheFile;               ///< cache file name to save clusters
    bool _readCache;                      ///< default: false
    bool _writeCache;                     ///< default: false
    int32_t _writeInterval;               ///< in seconds, default 5 minutes
    int _conhashWeightRatio;              ///< default 1 (enable weight conhash)
    uint32_t _virtualNodeRatio;           // 虚拟节点的比例, 默认0 disable
    bool _disableTopo;                    // 关闭TopoCluster构建，用于multi-call

private:
    int32_t parseCMServer(mxml_node_t* keynode);
    int32_t parseSubscriber(mxml_node_t* keynode);
    void parseCache(mxml_node_t* keynode);

public:
    SubscriberConfig();
    ~SubscriberConfig();
    int32_t init(const char* config_path);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace cm_sub

#endif
