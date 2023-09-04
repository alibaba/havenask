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
 *       Filename:  cm_central_hb.h
 *
 *    Description:  心跳管理使用的底层库
 *
 *        Version:  0.1
 *        Created:  2013-03-27 15:11:09
 *       Revision:  none
 *       Compiler:  glibc
 *
 *         Author:  tiechou (search engine group), tiechou@taobao.com
 *        Company:  www.taobao.com
 *
 * =====================================================================================
 */

#ifndef HB_NODE_MANAGER_CM_CENTRAL_IMP_H_
#define HB_NODE_MANAGER_CM_CENTRAL_IMP_H_

#include "aios/apps/facility/cm2/cm_basic/basic_struct/cm_central.h"
#include "autil/Log.h"

namespace cm_basic {

class CMCentralHb : public virtual CMCentral
{
    friend class CMCentralHbTest;

public:
    CMCentralHb() {}
    virtual ~CMCentralHb() {}

public:
    /**
     * @brief   设置一个节点的 offline/online
     * @param   [in]    节点Spec
     * @param   [in]    节点是否在线状态
     * @return  0:成功； -1:失败
     */
    int32_t setNodeValid(const std::string& nodeSpec, const std::string& rstrClusterName, OnlineStatus nodeStatus);
    /**
     * @brief 设置多个下线节点的 offline/online，一般会再写完binlog后调用。
     * @param pBinLog[in] 下线节点的bin_log信息数组
     * @param nSize[in] 数组元素个数
     * @return RSP_SUCCES成功，其他失败
     */
    RespStatus setNodesValid(BinLogUnit* pBinLog, int32_t nSize);

    void setNodeStatus(CMNodeWrapperPtr& pNode, NodeStatus status, const CMClusterWrapperPtr& pCluster);

    /**
     * @brief   设置一个集群的 offline/online
     * @param   [in]    集群名
     * @param   [in]    集群是否在线的状态
     * @return  0:成功； -1:失败
     */
    RespStatus setClusterStatus(const std::string& clusterName, OnlineStatus clusterStatus);
    /**
     * @brief 更新或增加一个集群
     * @param pCmCluster[in] 集群指针
     * @return RSP_SUCCESS为成功，其他为失败
     */
    RespStatus setCluster(CMCluster* pCmCluster);
    /*
     * @brief   检查所有集群是否超时
     * @param   [in]    超时时间间隔
     * @param   [in]    cluster收集node状态的时间间隔，默认1分钟(微秒)
     * @return  0:成功； -1:失败
     */
    int32_t checkTimeOut(int64_t timeoutInterval, int64_t clusterInitingInterval = 60000000);
    // 7层健康检查时，设置node对应的status
    int32_t set7LevelNodeValid(const std::string& nodeSpec, const std::string& clusterName, NodeStatus nodeStatus);

    /*
     * @brief   4/7 层健康检查时,取相应 CMCluster::NodeCheckType 的所有集群
     * @param   [in/out]    vecCluster
     * @param   [in]    CMCluster::NodeCheckType checkType
     */
    void getLevelNCheckCluster(std::vector<CMClusterWrapperPtr>& vecCluster, CMCluster::NodeCheckType checkType);

    /*
     * @brief   获取ClusterUpdateInfo
     * @param   集群名
     * @param   序列化后的集群
     * @return  0:成功； -1:失败
     */
    int32_t getClusterUpdateInfo(const std::string& clusterName, SerializedCluster* sCluster, int32_t sysProtect = -1);
    bool serializeCluster(SerializedCluster* sCluster, ClusterUpdateInfo* updateInfo);

private:
    void setNodeStateChangeTime(CMNodeWrapperPtr& node, int64_t curTime, NodeStatus& curStatus);
    void adjustNodeStatus(CMClusterWrapperPtr& clusterWrapper, CMNodeWrapperPtr& node);
    bool inProtect(CMClusterWrapperPtr& pCluster, int32_t sysProtect);
    NodeStatus getProtectStatus(NodeStatus status, bool protect);

private:
    AUTIL_LOG_DECLARE();
    static alog::Logger* _nodeStatusChangeLogger;
};

} // namespace cm_basic

#endif // HB_NODE_MANAGER_CM_CENTRAL_IMP_H_
