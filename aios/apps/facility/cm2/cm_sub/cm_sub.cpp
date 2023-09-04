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
#include "aios/apps/facility/cm2/cm_sub/cm_sub.h"

#include "aios/apps/facility/cm2/cm_basic/basic_struct/cm_central_sub.h"
#include "aios/apps/facility/cm2/cm_basic/common/zk_option.h"
#include "aios/apps/facility/cm2/cm_basic/leader_election/master_server.h"
#include "aios/apps/facility/cm2/cm_sub/cm_sub_imp.h"
#include "aios/apps/facility/cm2/cm_sub/config/subscriber_config.h"
#include "aios/apps/facility/cm2/cm_sub/local_sub_imp.h"
#include "aios/apps/facility/cm2/cm_sub/server_resolver.h"
#include "aios/apps/facility/cm2/cm_sub/topo_cluster.h"
#include "aios/apps/facility/cm2/cm_sub/topo_cluster_manager.h"
#include "aios/network/arpc/arpc/ANetRPCChannel.h"
#include "aios/network/arpc/arpc/ANetRPCChannelManager.h"

namespace cm_sub {

AUTIL_LOG_SETUP(cm_sub, CMSubscriber);

CMSubscriber::CMSubscriber() : QueryInterface(), _serverResolver(NULL), _arpc(NULL), _cfgInfo(NULL), _cmSubImp(NULL) {}

CMSubscriber::~CMSubscriber() { release(); }

void CMSubscriber::release()
{
    if (_cmSubImp)
        _cmSubImp->unsubscriber();

    // 在 _cmSubImp， 释放之后 释放arpc
    if (_arpc)
        _arpc->StopPrivateTransport();

    deletePtr(_cmSubImp);
    deletePtr(_arpc);

    deletePtr(_topoClusterManager);
    deletePtr(_cmCentral);

    deletePtr(_serverResolver);
    deletePtr(_cfgInfo);
}

int32_t CMSubscriber::init(const char* config_path, const char* node_spec)
{
    SubscriberConfig* cfg_info = new (std::nothrow) SubscriberConfig();
    if (cfg_info == NULL || cfg_info->init(config_path) != 0) {
        AUTIL_LOG(ERROR, "cm_sub : CMSubscriber::init(), cfg_info->init(%s) failed !!", config_path);
        deletePtr(cfg_info);
        return -1;
    }
    return init(cfg_info, node_spec);
}
int32_t CMSubscriber::init(SubscriberConfig* cfg_info, const char* node_spec)
{
    _cfgInfo = cfg_info;
    // 初始化 拓扑集群
    _topoClusterManager =
        new (std::nothrow) TopoClusterManager(cfg_info->_conhashWeightRatio, cfg_info->_virtualNodeRatio);
    if (_topoClusterManager == NULL || _topoClusterManager->init() != 0) {
        AUTIL_LOG(ERROR, "cm_sub : CMSubscriber::init(), create _topoClusterManager failed !!");
        return -1;
    }
    // 初始化 cm_central
    _cmCentral = new (std::nothrow) cm_basic::CMCentralSub();
    if (_cmCentral == NULL) {
        AUTIL_LOG(ERROR, "cm_sub : CMSubscriber::init(), CMCentral create failed !!");
        return -1;
    }

    if (_cfgInfo->_isLocalMode == true) {
        // 初始化 subscriber实现类
        _cmSubImp = new (std::nothrow) LocalSubscriberImp(_cfgInfo);
        if (_cmSubImp == NULL || _cmSubImp->init(_topoClusterManager, _cmCentral) != 0) {
            AUTIL_LOG(ERROR, "cm_sub : LocalSubscriber _cmSubImp->init failed !!");
            return -1;
        }
        return 0;
    }

    if (_cfgInfo->_serverType == FromZK) {
        _serverResolver =
            new ZKServerResolver(_cfgInfo->_zkServer, _cfgInfo->_zkPath, _cfgInfo->_timeout, -1, _cfgInfo->_subMaster);
    } else if (_cfgInfo->_serverType == FromCfg) {
        _serverResolver = new ConfServerResolver(_cfgInfo->_cmServer);
    }
    //
    if (_serverResolver == NULL || !_serverResolver->init()) {
        AUTIL_LOG(ERROR, "serverResolver->init() failed or not created");
        return -1;
    }

    // 初始化 arpc
    _arpc = new (std::nothrow) arpc::ANetRPCChannelManager();
    if (_arpc == NULL || _arpc->StartPrivateTransport() == false) {
        AUTIL_LOG(ERROR, "cm_sub : CMSubscriber _arpc->StartPrivateTransport() failed !!");
        return -1;
    }
    // 初始化 subscriber实现类
    _cmSubImp = new (std::nothrow) CMSubscriberImp(_cfgInfo, _arpc, _serverResolver);
    if (_cmSubImp == NULL || _cmSubImp->init(_topoClusterManager, _cmCentral) != 0) {
        AUTIL_LOG(ERROR, "cm_sub : CMSubscriber _cmSubImp->init failed !!");
        return -1;
    }
    return 0;
}

int32_t CMSubscriber::subscriber()
{
    if (_cmSubImp->subscriber() != 0) {
        AUTIL_LOG(ERROR, "cm_sub : CMSubscriber _cmSubImp->subscriber() failed !!");
        return -1;
    }
    return 0;
}

int32_t CMSubscriber::addSubCluster(std::string name) { return _cmSubImp->addSubCluster(name); }

int32_t CMSubscriber::removeSubCluster(std::string name) { return _cmSubImp->removeSubCluster(name); }

} // namespace cm_sub
