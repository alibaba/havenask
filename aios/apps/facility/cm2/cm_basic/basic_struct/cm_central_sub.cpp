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

#include "aios/apps/facility/cm2/cm_basic/basic_struct/cm_central_sub.h"

namespace cm_basic {

// 需要对node_online_status进行兼容性考虑
int32_t CMCentralSub::updateClusterNodeStatus(const std::string& cluster_name, NodeStatusVec* status_vec)
{
    if (unlikely(NULL == status_vec)) {
        return -1;
    }
    auto cluster_wrapper = getClusterWrapper(cluster_name);
    if (unlikely(cluster_wrapper == nullptr)) {
        return -1;
    }
    // 集群内部节点数量 与 更新信息的节点数量不相等
    auto node_list = cluster_wrapper->getNodeList();
    if (node_list.size() != status_vec->node_status_vec_size()) {
        return -1;
    }
    assert(status_vec->node_online_status_size() == 0 || // compatible: 0 or node_num
           status_vec->node_online_status_size() == node_list.size());
    cluster_wrapper->setClusterStatus(status_vec->cluster_status());
    // 按顺序更新每个结点的状态
    for (int32_t i = 0; i < node_list.size(); i++) {
        assert(status_vec->node_online_status_size() == 0 || // compatible
               (status_vec->node_online_status(i) == OS_OFFLINE && status_vec->node_status_vec(i) == NS_UNVALID) ||
               status_vec->node_online_status(i) == OS_ONLINE);
        node_list[i]->setCurStatus(status_vec->node_status_vec(i));
        if (status_vec->node_online_status_size() > 0) {
            node_list[i]->setOnlineStatus(status_vec->node_online_status(i));
        }
    }
    return 0;
}

int32_t CMCentralSub::updateClusterNodeLBInfo(const std::string& cluster_name, NodeLBInfoVec* lb_info_vec)
{
    if (unlikely(NULL == lb_info_vec)) {
        return -1;
    }
    auto cluster_wrapper = getClusterWrapper(cluster_name);
    if (unlikely(cluster_wrapper == nullptr)) {
        return -1;
    }
    // 集群内部节点数量 与 更新信息的节点数量不相等
    auto node_list = cluster_wrapper->getNodeList();
    if (node_list.size() != lb_info_vec->lb_info_vec_size()) {
        return -1;
    }
    for (uint32_t i = 0; i < node_list.size(); i++) {
        node_list[i]->setLBInfo(lb_info_vec->lb_info_vec(i));
    }

    if (node_list.size() != lb_info_vec->meta_info_vec_size()) {
        return 0;
    }
    for (uint32_t i = 0; i < node_list.size(); i++) {
        node_list[i]->setMetaInfo(lb_info_vec->meta_info_vec(i));
    }
    return 0;
}

RespStatus CMCentralSub::updateCluster(CMCluster* p_cm_cluster)
{
    if (unlikely(NULL == p_cm_cluster)) {
        return RSP_FAILED;
    }
    autil::ScopedWriteLock lock(_rwlock);
    CMClusterWrapperPtr cluster_wrapper(new CMClusterWrapper(p_cm_cluster));
    _cluster_map[p_cm_cluster->cluster_name()] = cluster_wrapper;
    return RSP_SUCCESS;
}

OnlineStatus CMCentralSub::getClusterStatus(const std::string& cluster_name)
{
    auto cluster_wrapper = getClusterWrapper(cluster_name);
    if (unlikely(nullptr == cluster_wrapper)) {
        return OS_OFFLINE;
    }
    return cluster_wrapper->getClusterStatus();
}

int32_t CMCentralSub::getClusterNodeStatus(const std::string& cluster_name, NodeStatusVec*& status_vec)
{
    auto cluster_wrapper = getClusterWrapper(cluster_name);
    if (unlikely(cluster_wrapper == nullptr)) {
        return -1;
    }
    status_vec = new NodeStatusVec();
    assert(status_vec != NULL);

    auto node_list = cluster_wrapper->getNodeList();
    status_vec->set_cluster_status(cluster_wrapper->getClusterStatus());
    for (auto& node : node_list) {
        if (node->getOnlineStatus() == OS_ONLINE) {
            status_vec->add_node_status_vec(node->getCurStatus());
        } else {
            status_vec->add_node_status_vec(NS_UNVALID);
        }
        status_vec->add_node_online_status(node->getOnlineStatus());
    }
    return 0;
}

int32_t CMCentralSub::getClusterNodeLBInfo(const std::string& cluster_name, NodeLBInfoVec*& lb_info_vec)
{
    auto cluster_wrapper = getClusterWrapper(cluster_name);
    if (unlikely(cluster_wrapper == nullptr)) {
        return -1;
    }
    lb_info_vec = new NodeLBInfoVec();
    assert(lb_info_vec != NULL);

    auto node_list = cluster_wrapper->getNodeList();
    for (auto& node : node_list) {
        if (node->hasLBInfo() == true) {
            lb_info_vec->add_lb_info_vec()->CopyFrom(node->getLBInfo());
        } else {
            lb_info_vec->add_lb_info_vec();
        }
        if (node->hasMetaInfo() == true) {
            lb_info_vec->add_meta_info_vec()->CopyFrom(node->getMetaInfo());
        } else {
            lb_info_vec->add_meta_info_vec();
        }
    }
    return 0;
}

int32_t CMCentralSub::getNodeStatus(const std::string& node_spec, const std::string& rstr_cluster_name,
                                    NodeStatus& node_status)
{
    auto cluster_wrapper = getClusterWrapper(rstr_cluster_name);
    if (unlikely(cluster_wrapper == nullptr)) {
        return -1;
    }
    auto node = cluster_wrapper->findNode(node_spec);
    if (node == nullptr) {
        return -1;
    }
    if (node->getOnlineStatus() == OS_ONLINE) {
        node_status = node->getCurStatus();
    } else {
        node_status = NS_UNVALID;
    }
    return 0;
}

int32_t CMCentralSub::getNodeMetaInfo(const std::string& node_spec, const std::string& rstr_cluster_name,
                                      NodeMetaInfo* meta_info)
{
    if (unlikely(NULL == meta_info)) {
        return -1;
    }
    auto cluster_wrapper = getClusterWrapper(rstr_cluster_name);
    if (unlikely(cluster_wrapper == nullptr)) {
        return -1;
    }
    auto node = cluster_wrapper->findNode(node_spec);
    if (node == nullptr) {
        return -1;
    }
    if (node->hasMetaInfo()) {
        meta_info->CopyFrom(node->getMetaInfo());
    }
    return 0;
}

} // namespace cm_basic
