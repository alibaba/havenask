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
 *       Filename:  cm_sub.h
 *
 *    Description:  客户端基础库，订阅集群信息，提供查询接口
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

#ifndef __CM_SUBSCRIBER_H_
#define __CM_SUBSCRIBER_H_

#include "aios/apps/facility/cm2/cm_sub/query_interface.h"
#include "aios/apps/facility/cm2/cm_sub/service_node.h"
#include "autil/Log.h"

namespace cm_basic {
class CMCentralSub;
class IMasterServer;
} // namespace cm_basic

namespace arpc {
class ANetRPCChannelManager;
}

namespace cm_sub {

class SubscriberConfig;
class CMSubscriberBase;
class TopoClusterManager;
class IServerResolver;

/**
 * @class CMSubscriber
 * @brief 订阅者接口类
 */
class CMSubscriber : public QueryInterface
{
private:
    IServerResolver* _serverResolver;
    arpc::ANetRPCChannelManager* _arpc; ///< arpc
    SubscriberConfig* _cfgInfo;         ///< 配置项
    CMSubscriberBase* _cmSubImp;        ///< 订阅实现

public:
    CMSubscriber();
    ~CMSubscriber();
    /**
     * @brief   获取cm_central
     * @return  返回cm_central
     */
    cm_basic::CMCentralSub* getCMCentral() { return _cmCentral; }
    /**
     * @brief   获取topo_cluster_manager
     * @return  返回topo_cluster_manager
     */
    TopoClusterManager* getTopoClusterManager() { return _topoClusterManager; }
    /**
     * @brief   初始化 Subscriber
     * @param   [in] config_path Subscriber配置文件cm_client.xml
     * @param   [in] node_spec 如果当前节点即是HBNode 又是Subscriber，sub_spec 不能为空
     * @return  0: success,  -1: failed
     */
    int32_t init(const char* config_path, const char* node_spec = NULL);
    /**
     * @brief   初始化 Subscriber
     * @param   [in] cfg_info 初始化完成的配置信息对象，外部申请内部释放
     * @param   [in] node_spec 如果当前节点即是HBNode 又是Subscriber，sub_spec 不能为空
     * @return  0: success,  -1: failed
     */
    int32_t init(SubscriberConfig* cfg_info, const char* node_spec = NULL);
    /**
     * @brief 开始订阅
     */
    int32_t subscriber();
    /**
     * @brief 释放资源
     */
    void release();
    /**
     * @brief 增加集群
     */
    int32_t addSubCluster(std::string name);
    /**
     * @brief 删除集群
     */
    int32_t removeSubCluster(std::string name);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace cm_sub
#endif
