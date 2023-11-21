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
#include "aios/apps/facility/cm2/cm_basic/basic_struct/cm_central_hb.h"

#include <assert.h>

#include "aios/apps/facility/cm2/cm_basic/util/log.h"
#include "aios/apps/facility/cm2/cm_basic/util/str_util.h"

namespace cm_basic {

AUTIL_LOG_SETUP(cm_basic, CMCentralHb);
alog::Logger* CMCentralHb::_nodeStatusChangeLogger = alog::Logger::getLogger(cm_basic::CM_BASIC_NODE_STATUS_CHANGE_LOG);
#ifndef AUTIL_NODE_STATUS_CHANGE_LOG
#define AUTIL_NODE_STATUS_CHANGE_LOG(fmt, args...) _nodeStatusChangeLogger->log(alog::LOG_LEVEL_INFO, fmt, ##args);
#endif // AUTIL_NODE_STATUS_CHANGE_LOG

int32_t CMCentralHb::setNodeValid(const std::string& node_spec, const std::string& cluster_name,
                                  OnlineStatus node_status)
{
    assert(node_status != OS_INITING);
    int64_t cur_time = autil::TimeUtility::currentTime();
    auto cluster_wrapper = getClusterWrapper(cluster_name);
    if (unlikely(cluster_wrapper == nullptr)) {
        return -1;
    }
    auto node = cluster_wrapper->findNode(node_spec);
    if (node == nullptr) {
        return -1;
    }
    node->setOnlineStatus(node_status);
    adjustNodeStatus(cluster_wrapper, node);
    node->setValidTime(cur_time);
    return 0;
}

RespStatus CMCentralHb::setNodesValid(BinLogUnit* p_bin_log, int32_t n_size)
{
    if (unlikely(NULL == p_bin_log || n_size <= 0)) {
        return RSP_FAILED;
    }
    for (int32_t i = 0; i < n_size; ++i) {
        assert(p_bin_log->state() != OS_INITING);
        if (setNodeValid(p_bin_log[i].node_spec(), p_bin_log[i].cluster_name(), p_bin_log->state()) != 0) {
            return RSP_NODE_NOT_EXIST;
        }
    }
    return RSP_SUCCESS;
}

RespStatus CMCentralHb::setClusterStatus(const std::string& cluster_name, OnlineStatus cluster_status)
{
    auto cluster_wrapper = getClusterWrapper(cluster_name);
    if (unlikely(cluster_wrapper == nullptr)) {
        return RSP_CLUSTER_NOT_EXIST;
    }
    cluster_wrapper->setClusterStatus(cluster_status);
    return RSP_SUCCESS;
}

RespStatus CMCentralHb::setCluster(CMCluster* p_cm_cluster)
{
    if (unlikely(NULL == p_cm_cluster)) {
        return RSP_FAILED;
    }
    autil::ScopedWriteLock lock(_rwlock);
    auto iter = _cluster_map.find(p_cm_cluster->cluster_name());
    if (iter != _cluster_map.end()) {
        {
            p_cm_cluster->set_cluster_status(iter->second->getClusterStatus());
        }
        _cluster_map.erase(iter);
    }
    if (p_cm_cluster->cluster_status() == cm_basic::OS_ONLINE) {
        p_cm_cluster->set_cluster_status(cm_basic::OS_INITING);
    }
    CMClusterWrapperPtr cluster_wrapper(new CMClusterWrapper(p_cm_cluster));
    _cluster_map[p_cm_cluster->cluster_name()] = cluster_wrapper;
    return RSP_SUCCESS;
}

int32_t CMCentralHb::checkTimeOut(int64_t timeoutInterval, int64_t clusterInitingInterval)
{
    int64_t curTime = autil::TimeUtility::currentTime();
    autil::ScopedWriteLock lock(_rwlock);
    for (auto& iter : _cluster_map) {
        // 定期将CMCluster的状态从OS_INITING状态转换成OS_ONLINE, 并初始化版本
        CMClusterWrapperPtr& clusterWrapper = iter.second;
        if (OS_INITING == clusterWrapper->getClusterStatus() &&
            curTime - clusterWrapper->getClusterCreateTime() > clusterInitingInterval) {
            clusterWrapper->setClusterStatus(OS_ONLINE);
            clusterWrapper->setClusterVersion(curTime);
        }
    }
    return 0;
}

int32_t CMCentralHb::set7LevelNodeValid(const std::string& node_spec, const std::string& cluster_name,
                                        NodeStatus node_status)
{
    auto cluster_wrapper = getClusterWrapper(cluster_name);
    if (unlikely(cluster_wrapper == nullptr)) {
        AUTIL_NODE_STATUS_CHANGE_LOG("CMCentralHb::set7LevelNodeValid(): node not exist! spec=%s|%s",
                                     cluster_name.c_str(), node_spec.c_str());
        return -1;
    }
    auto p_node = cluster_wrapper->findNode(node_spec);
    if (unlikely(p_node == nullptr)) {
        AUTIL_NODE_STATUS_CHANGE_LOG("CMCentralHb::set7LevelNodeValid(): node not exist! spec=%s|%s",
                                     cluster_name.c_str(), node_spec.c_str());
        return -1;
    }
    setNodeStatus(p_node, node_status, cluster_wrapper);
    return 0;
}

void CMCentralHb::setNodeStatus(CMNodeWrapperPtr& pNode, NodeStatus status, const CMClusterWrapperPtr& pCluster)
{
    assert(pNode);
    int64_t curTime = autil::TimeUtility::currentTime();
    NodeStatus curStatus = NS_UNVALID;
    NodeStatus preStatus = pNode->getPrevStatus();
    if (pNode->getOnlineStatus() == OS_OFFLINE) {
        status = NS_UNVALID;
    }

    // 每次setNodeStatus调用完成之后, curStatus会和preStatus保持一致, 都是这一轮检查的结果
    pNode->setCurStatus(status);
    setNodeStateChangeTime(pNode, curTime, curStatus);
    pNode->setPrevStatus(status);

    if (status != preStatus && pCluster && pCluster->getClusterVersion() != 0) {
        // do not record node status change on recovery from zk
        // only record node status change on inited cluster
        std::string nodeSpec = pNode->getNodeSpec();
        AUTIL_NODE_STATUS_CHANGE_LOG("nodespec=%s clustername=%s node_status change %s to %s", nodeSpec.c_str(),
                                     pNode->getClusterName().c_str(), enum_node_status[preStatus],
                                     enum_node_status[status]);
    }
}

void CMCentralHb::getLevelNCheckCluster(std::vector<CMClusterWrapperPtr>& vecCluster,
                                        CMCluster::NodeCheckType checkType)
{
    vecCluster.clear();

    autil::ScopedReadLock lock(_rwlock);
    for (auto& iter : _cluster_map) {
        if (iter.second->getClusterCheckType() != checkType) {
            continue;
        }
        vecCluster.push_back(iter.second);
    }
}

int32_t CMCentralHb::getClusterUpdateInfo(const std::string& cluster_name, SerializedCluster* s_cluster,
                                          int32_t sys_protect)
{
    ClusterUpdateInfo update_info;
    update_info.set_cluster_name(cluster_name);

    auto cluster_wrapper = getClusterWrapper(cluster_name);
    if (cluster_wrapper == nullptr) {
        // 第一轮查询, 该集群已经被删除，版本号设置为0
        s_cluster->msg_type = ClusterUpdateInfo::MT_CLUSTER_DEL;
        s_cluster->version = 0;
        return serializeCluster(s_cluster, &update_info);
    }
    if (cluster_wrapper->getClusterStatus() == OS_INITING) {
        // 该集群正在 构建，版本号设置为当前版本号
        s_cluster->msg_type = ClusterUpdateInfo::MT_CLUSTER_BUILDING;
        s_cluster->version = cluster_wrapper->getClusterVersion();
        return serializeCluster(s_cluster, &update_info);
    }

    if (cluster_wrapper->getClusterVersion() > s_cluster->version) {
        // 集群拓扑结构有更新
        s_cluster->msg_type = ClusterUpdateInfo::MT_CLUSTER_REINIT;
        s_cluster->version = cluster_wrapper->getClusterVersion();
        cluster_wrapper->CopyTo(update_info.mutable_cm_cluster());
        return serializeCluster(s_cluster, &update_info);
    }

    bool protect = inProtect(cluster_wrapper, sys_protect);
    auto node_list = cluster_wrapper->getNodeList();
    if (s_cluster->msg_type == ClusterUpdateInfo::MT_NODE_STATUS) {
        // 指定需要 结点状态信息
        NodeStatusVec* status_vec = update_info.mutable_node_status_vec();
        // 设置集群状态
        status_vec->set_cluster_status(cluster_wrapper->getClusterStatus());
        // 添加每个节点的状态到 NodeStatusVec
        for (auto& node : node_list) {
            if (node->getOnlineStatus() == OS_ONLINE) {
                status_vec->add_node_status_vec(getProtectStatus(node->getCurStatus(), protect));
            } else {
                status_vec->add_node_status_vec(NS_UNVALID);
            }
            assert(node->getOnlineStatus() != OS_OFFLINE ||
                   (node->getOnlineStatus() == OS_OFFLINE && node->getCurStatus() == NS_UNVALID));
            status_vec->add_node_online_status(node->getOnlineStatus());
        }
    } else {
        AUTIL_LOG(ERROR, "unsupport cluster update info type %d", s_cluster->msg_type);
    }
    return serializeCluster(s_cluster, &update_info);
}

bool CMCentralHb::serializeCluster(SerializedCluster* s_cluster, ClusterUpdateInfo* update_info)
{
    update_info->set_msg_type(s_cluster->msg_type);
    update_info->set_cluster_version(s_cluster->version);

    bool ret = true;
    ///< 缓存序列化后信息
    if (s_cluster->msg_type == ClusterUpdateInfo::MT_CLUSTER_REINIT) {
        s_cluster->releaseCMCluster();
        (s_cluster->cluster_len)[CT_NONE] = update_info->ByteSize();
        (s_cluster->cm_cluster)[CT_NONE] = new char[update_info->ByteSize()];
        ret = update_info->SerializeToArray((s_cluster->cm_cluster)[CT_NONE], update_info->ByteSize());
    } else {
        s_cluster->releaseClusterStatus();
        (s_cluster->status_len)[CT_NONE] = update_info->ByteSize();
        (s_cluster->cluster_status)[CT_NONE] = new char[update_info->ByteSize()];
        ret = update_info->SerializeToArray((s_cluster->cluster_status)[CT_NONE], update_info->ByteSize());
    }
    return ret;
}

NodeStatus CMCentralHb::getProtectStatus(NodeStatus status, bool protect)
{
    return !protect ? status : (status == NS_TIMEOUT ? NS_NORMAL : status);
}

bool CMCentralHb::inProtect(CMClusterWrapperPtr& cm_cluster, int32_t sys_protect)
{
    // 无效(timeout)节点比例>=该值则进入保护模式
    int32_t ratio = sys_protect;
    if (sys_protect == -2)
        ratio = cm_cluster->getProtectRatio();

    if (ratio == -1)
        return false; // disable

    int32_t total = 0, timeout = 0;
    auto node_map = cm_cluster->getNodeMap();
    for (auto& iter : node_map) {
        if (iter.second->getOnlineStatus() != OS_ONLINE) {
            continue;
        }
        total++;
        NodeStatus status = iter.second->getCurStatus();
        if (status == NS_TIMEOUT) {
            timeout++;
        }
    }
    bool ret = total > 0 && timeout * 100 / total >= ratio;
    if (ret) {
        AUTIL_LOG(WARN, "protect node status for %s total: %d, timeout: %d, ratio: %d",
                  cm_cluster->getClusterName().c_str(), total, timeout, ratio);
    }
    return ret;
}

void CMCentralHb::setNodeStateChangeTime(CMNodeWrapperPtr& node, int64_t curTime, NodeStatus& curStatus)
{
    curStatus = node->getCurStatus();
    if (OS_OFFLINE == node->getOnlineStatus()) {
        curStatus = NS_UNVALID;
    }
    // 节点状态第一次有效后，记录时间，为开始时间
    if (node->getStartTime() <= 0 && NS_NORMAL == curStatus) {
        node->setStartTime(curTime);
    }
}

void CMCentralHb::adjustNodeStatus(CMClusterWrapperPtr& cluster, CMNodeWrapperPtr& node)
{
    if (node->getOnlineStatus() == OS_OFFLINE) {
        node->setCurStatus(NS_UNVALID);
    } else if (node->getOnlineStatus() == OS_ONLINE) {
        if (cluster->getClusterCheckType() == CMCluster::NCT_KEEP_ONLINE) {
            node->setCurStatus(NS_NORMAL);
        }
    }
}

} // namespace cm_basic
