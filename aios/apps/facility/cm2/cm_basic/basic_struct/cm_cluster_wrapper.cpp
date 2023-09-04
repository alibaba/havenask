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
#include "aios/apps/facility/cm2/cm_basic/basic_struct/cm_cluster_wrapper.h"

namespace cm_basic {

CMClusterWrapper::CMClusterWrapper(CMCluster* cm_cluster) : _cm_cluster(cm_cluster)
{
    assert(_cm_cluster != nullptr);
    initNodeMap();
}

CMClusterWrapper::~CMClusterWrapper()
{
    autil::ScopedWriteLock lock(_rwlock);
    _node_map.clear();
    delete _cm_cluster;
    _cm_cluster = nullptr;
}

void CMClusterWrapper::initNodeMap()
{
    CMGroup* cm_group = NULL;
    CMNode* cm_node = NULL;
    std::string spec;

    _node_map.clear();
    _node_list.clear();
    _group_node_list.clear();

    uint32_t g_cnt = _cm_cluster->group_vec_size();
    for (uint32_t i = 0; i < g_cnt; ++i) {
        cm_group = _cm_cluster->mutable_group_vec(i);
        uint32_t n_cnt = cm_group->node_vec_size();
        std::vector<CMNodeWrapperPtr> node_vec;
        for (uint32_t j = 0; j < n_cnt; ++j) {
            cm_node = cm_group->mutable_node_vec(j);
            CMNodeWrapper::makeSpec(cm_node, spec);
            CMNodeWrapperPtr node_wrapper(new CMNodeWrapper(spec, cm_node));
            _node_map.emplace(std::make_pair(spec, node_wrapper));
            _node_list.push_back(node_wrapper);
            node_vec.push_back(node_wrapper);
        }
        _group_node_list.push_back(node_vec);
    }
}

RespStatus CMClusterWrapper::cloneCluster(CMCluster*& cm_cluster)
{
    autil::ScopedReadLock lock(_rwlock);
    cm_cluster = new (std::nothrow) CMCluster(*_cm_cluster);
    if (unlikely(NULL == cm_cluster)) {
        return RSP_FAILED;
    }
    return RSP_SUCCESS;
}

CMNode* CMClusterWrapper::cloneNode(const std::string& node_spec)
{
    auto node = findNode(node_spec);
    if (node == nullptr) {
        return nullptr;
    }
    return node->cloneNode();
}

cm_basic::RespStatus CMClusterWrapper::checkNode(const CMNode& node)
{
    if (node.group_id() >= getClusterGroupVecSize()) {
        return cm_basic::RSP_PARTITION_NOT_EXIST;
    }
    return cm_basic::RSP_SUCCESS;
}

int32_t CMClusterWrapper::getClusterNodeList(std::vector<CMNode*>& node_vec)
{
    autil::ScopedReadLock lock(_rwlock);

    // 遍历集群的每一个节点
    uint32_t g_cnt = _cm_cluster->group_vec_size();
    for (uint32_t i = 0; i < g_cnt; ++i) {
        auto& cm_group = _cm_cluster->group_vec(i);
        uint32_t n_cnt = cm_group.node_vec_size();
        for (uint32_t j = 0; j < n_cnt; ++j) {
            auto& cm_node = cm_group.node_vec(j);
            CMNode* node = new (std::nothrow) CMNode(cm_node);
            assert(node != NULL);
            node_vec.push_back(node);
        }
    }
    return 0;
}

int CMClusterWrapper::mergeCluster(CMCluster* cluster, const std::string& cname)
{
    int node_num = 0;
    autil::ScopedReadLock lock(_rwlock);
    for (int gi = 0; gi < _cm_cluster->group_vec_size(); ++gi) {
        auto& src_group = _cm_cluster->group_vec(gi);
        // add a new group if it doesn't exist
        CMGroup* group = gi >= cluster->group_vec_size() ? cluster->add_group_vec() : cluster->mutable_group_vec(gi);
        for (int ni = 0; ni < src_group.node_vec_size(); ++ni) {
            // copy a node with new cluster name
            auto& src_node = src_group.node_vec(ni);
            CMNode* dst_node = group->add_node_vec();
            dst_node->CopyFrom(src_node);
            dst_node->set_cluster_name(cname);
            node_num++;
        }
    }
    return node_num;
}

std::map<std::string, CMNodeWrapperPtr> CMClusterWrapper::getNodeMap()
{
    autil::ScopedReadLock lock(_rwlock);
    return _node_map;
}

std::vector<CMNodeWrapperPtr> CMClusterWrapper::getNodeList()
{
    autil::ScopedReadLock lock(_rwlock);
    return _node_list;
}

const std::vector<CMNodeWrapperPtr>& CMClusterWrapper::getNodeListRef()
{
    autil::ScopedReadLock lock(_rwlock);
    return _node_list;
}

std::vector<std::vector<CMNodeWrapperPtr>> CMClusterWrapper::getGroupNodeList()
{
    autil::ScopedReadLock lock(_rwlock);
    return _group_node_list;
}

CMNodeWrapperPtr CMClusterWrapper::findNode(const std::string& node_spec)
{
    autil::ScopedReadLock lock(_rwlock);

    auto iter = _node_map.find(node_spec);
    if (iter != _node_map.end()) {
        return iter->second;
    }
    return CMNodeWrapperPtr();
}

void CMClusterWrapper::setClusterStatus(OnlineStatus cluster_status)
{
    autil::ScopedWriteLock lock(_rwlock);
    _cm_cluster->set_cluster_status(cluster_status);
}

void CMClusterWrapper::setClusterVersion(int64_t version)
{
    autil::ScopedWriteLock lock(_rwlock);
    _cm_cluster->set_version(version);
}

bool CMClusterWrapper::hasBelongClusters(const std::set<std::string>& belongs)
{
    autil::ScopedReadLock lock(_rwlock);
    for (int i = 0; i < _cm_cluster->belong_vec_size(); ++i) {
        const std::string& belong = _cm_cluster->belong_vec(i);
        if (belongs.find(belong) != belongs.end()) {
            return true;
        }
    }
    return false;
}

int64_t CMClusterWrapper::getClusterVersion()
{
    autil::ScopedReadLock lock(_rwlock);
    return _cm_cluster->version();
}

int32_t CMClusterWrapper::getClusterGroupVecSize()
{
    autil::ScopedReadLock lock(_rwlock);
    return _cm_cluster->group_vec_size();
}

CMCluster::NodeCheckType CMClusterWrapper::getClusterCheckType()
{
    autil::ScopedReadLock lock(_rwlock);
    return _cm_cluster->check_type();
}

CMCluster::TopoType CMClusterWrapper::getClusterTopoType()
{
    autil::ScopedReadLock lock(_rwlock);
    return _cm_cluster->topo_type();
}

bool CMClusterWrapper::getClusterAutoOffline()
{
    autil::ScopedReadLock lock(_rwlock);
    return _cm_cluster->has_auto_offline() && _cm_cluster->auto_offline();
}

OnlineStatus CMClusterWrapper::getClusterStatus()
{
    autil::ScopedReadLock lock(_rwlock);
    return _cm_cluster->cluster_status();
}

NodeLimit CMClusterWrapper::getClusterNodeLimits()
{
    autil::ScopedReadLock lock(_rwlock);
    return _cm_cluster->node_limits();
}

int64_t CMClusterWrapper::getClusterCreateTime()
{
    autil::ScopedReadLock lock(_rwlock);
    return _cm_cluster->create_time();
}

std::string CMClusterWrapper::getClusterName()
{
    autil::ScopedReadLock lock(_rwlock);
    return _cm_cluster->cluster_name();
}

int32_t CMClusterWrapper::getClusterNodeNum()
{
    autil::ScopedReadLock lock(_rwlock);
    return _cm_cluster->node_num();
}

std::string CMClusterWrapper::getClusterCheckFilename()
{
    autil::ScopedReadLock lock(_rwlock);
    return _cm_cluster->check_filename();
}

int32_t CMClusterWrapper::getBelongVecSize()
{
    autil::ScopedReadLock lock(_rwlock);
    return _cm_cluster->belong_vec_size();
}

std::string CMClusterWrapper::getBelong(int32_t i)
{
    autil::ScopedReadLock lock(_rwlock);
    return _cm_cluster->belong_vec(i);
}

bool CMClusterWrapper::hasProtectRatio()
{
    autil::ScopedReadLock lock(_rwlock);
    return _cm_cluster->has_protect_ratio();
}

int32_t CMClusterWrapper::getProtectRatio()
{
    autil::ScopedReadLock lock(_rwlock);
    return _cm_cluster->protect_ratio();
}

bool CMClusterWrapper::hasIDCPreferRatio()
{
    autil::ScopedReadLock lock(_rwlock);
    return _cm_cluster->has_idc_prefer_ratio();
}

int32_t CMClusterWrapper::getIDCPreferRatio()
{
    autil::ScopedReadLock lock(_rwlock);
    return _cm_cluster->idc_prefer_ratio();
}

bool CMClusterWrapper::hasIDCPreferRatioNew()
{
    autil::ScopedReadLock lock(_rwlock);
    return _cm_cluster->has_idc_prefer_ratio_new();
}

int32_t CMClusterWrapper::getIDCPreferRatioNew()
{
    autil::ScopedReadLock lock(_rwlock);
    return _cm_cluster->idc_prefer_ratio_new();
}

bool CMClusterWrapper::hasCloseIDCPreferNew()
{
    autil::ScopedReadLock lock(_rwlock);
    return _cm_cluster->has_close_idc_prefer_new();
}

bool CMClusterWrapper::getCloseIDCPreferNew()
{
    autil::ScopedReadLock lock(_rwlock);
    return _cm_cluster->close_idc_prefer_new();
}

void CMClusterWrapper::CopyTo(CMCluster* c)
{
    if (c) {
        autil::ScopedReadLock lock(_rwlock);
        c->CopyFrom(*_cm_cluster);
    }
}

bool CMClusterWrapper::CopyToGroup(int id, CMGroup* group)
{
    if (group == nullptr || id >= _cm_cluster->group_vec_size() || id < 0) {
        return false;
    }
    group->CopyFrom(_cm_cluster->group_vec(id));
    return true;
}

CMCluster* CMClusterWrapper::getCMCluster() { return _cm_cluster; }

size_t CMClusterWrapper::getNodeCount()
{
    autil::ScopedReadLock lock(_rwlock);
    return _node_list.size();
}

size_t CMClusterWrapper::getNormalNodeCount()
{
    size_t count = 0;
    autil::ScopedReadLock lock(_rwlock);
    for (auto& node : _node_list) {
        if (node->getCurStatus() == NS_NORMAL) {
            count++;
        }
    }
    return count;
}

} // namespace cm_basic