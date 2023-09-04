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
#include "aios/apps/facility/cm2/cm_sub/topo_cluster_manager.h"

#include <algorithm>
#include <stdlib.h>

#include "aios/apps/facility/cm2/cm_basic/util/str_util.h"
#include "aios/apps/facility/cm2/cm_sub/lb_policy/lb_policy.h"
#include "aios/apps/facility/cm2/cm_sub/service_node.h"
#include "aios/apps/facility/cm2/cm_sub/topo_cluster.h"

namespace cm_sub {

using namespace cm_basic;

TopoClusterManager::TopoClusterManager(int hashWeightRatio, uint32_t virtualNodeRatio)
    : TopoClusterTable(hashWeightRatio, virtualNodeRatio)
{
}

int32_t TopoClusterManager::init() { return 0; }

TopoClusterManager::~TopoClusterManager() {}

// we do the timeout in subscribing thread to make this function simple
void TopoClusterManager::onClusterQuery(const std::string& cluster_name)
{
    autil::ScopedLock lock(_queriedMutex);
    _queriedClusters[cluster_name] = autil::TimeUtility::currentTime();
}

int32_t TopoClusterManager::getPartitionCnt(const char* topo_cluster_name)
{
    if (topo_cluster_name == NULL) {
        return -1;
    }

    auto cluster = getTopoCluster(topo_cluster_name);
    if (cluster == NULL) {
        return -1;
    }

    return cluster->getPartitionCount();
}

int32_t TopoClusterManager::getNodeCntOfPartition(const char* topo_cluster_name, int32_t partition_id)
{
    if (topo_cluster_name == NULL) {
        return -1;
    }

    auto cluster = getTopoCluster(topo_cluster_name);
    if (cluster == NULL) {
        return -1;
    }

    return cluster->getNodeCntOfPartition(partition_id);
}

int32_t TopoClusterManager::getValidNodeCntOfPartition(const char* topo_cluster_name, int32_t partition_id)
{
    if (topo_cluster_name == NULL) {
        return -1;
    }

    auto cluster = getTopoCluster(topo_cluster_name);
    if (cluster == NULL) {
        return -1;
    }

    return cluster->getValidNodeCntOfPartition(partition_id);
}

int32_t TopoClusterManager::allocRow(const char* topo_cluster_name, std::vector<ServiceNode*>& node_vec,
                                     ProtocolType protocol, LBPolicyType policy, IDCType idc, uint32_t locating_signal)
{
    if (topo_cluster_name == NULL) {
        return -1;
    }

    auto cluster = getTopoCluster(topo_cluster_name);
    if (cluster == NULL) {
        return -1;
    }

    return cluster->allocRow(node_vec, protocol, idc, locating_signal, policy);
}

int32_t TopoClusterManager::allocValidRow(const char* topo_cluster_name, std::vector<ServiceNode*>& node_vec,
                                          ProtocolType protocol, LBPolicyType policy, IDCType idc,
                                          uint32_t locating_signal)
{
    if (topo_cluster_name == NULL) {
        return -1;
    }

    auto cluster = getTopoCluster(topo_cluster_name);
    if (cluster == NULL) {
        return -1;
    }

    return cluster->allocValidRow(node_vec, protocol, idc, locating_signal, policy);
}

int32_t TopoClusterManager::allocValidVersionRow(std::vector<ServiceNode*>& node_vec, long& version,
                                                 const char* topo_cluster_name, ProtocolType protocol,
                                                 LBPolicyType policy, uint32_t locating_signal)
{
    auto cluster = getTopoCluster(topo_cluster_name);
    if (cluster == NULL || node_vec.size() > 0) {
        return -1;
    }

    std::map<long, std::vector<ServiceNode*>> rows;
    long sel_version = cluster->allocValidVersionRow(rows, protocol, IDC_ANY, locating_signal, policy);
    if (sel_version == -1) {
        // no valid full rows found, try to find the best
        long row_ver = -1;
        size_t valid_cnt = 0; // valid partition count
        for (auto it = rows.begin(); it != rows.end(); ++it) {
            if (it->second.size() > valid_cnt) // found a better row (the greater valid partitions count)
            {
                valid_cnt = it->second.size();
                row_ver = it->first;
            }
        }
        sel_version = row_ver;
    }
    // do the output
    version = sel_version;
    if (sel_version != -1) // maybe still not found
    {
        node_vec = rows[sel_version];
    }
    // do the release
    for (auto it = rows.begin(); it != rows.end(); ++it) {
        if (it->first != sel_version) {
            for (auto n : it->second)
                delete n;
        }
    }
    return node_vec.size() > 0 ? 0 : -1;
}

int32_t TopoClusterManager::allocNodeOfPartition(const char* topo_cluster_name, int32_t partition_id,
                                                 std::vector<ServiceNode*>& node_vec, ProtocolType protocol,
                                                 IDCType idc)
{
    auto cluster = getTopoCluster(topo_cluster_name);
    if (cluster == NULL) {
        return -1;
    }
    return cluster->allocNodeOfPartition(node_vec, partition_id, protocol, idc);
}

int32_t TopoClusterManager::allocValidNodeOfPartition(const char* topo_cluster_name, int32_t partition_id,
                                                      std::vector<ServiceNode*>& node_vec, ProtocolType protocol,
                                                      IDCType idc)
{
    auto cluster = getTopoCluster(topo_cluster_name);
    if (cluster == NULL) {
        return -1;
    }
    return cluster->allocValidNodeOfPartition(node_vec, partition_id, protocol, idc);
}

int32_t TopoClusterManager::allocNodeByPartitionId(const char* topo_cluster_name, int32_t partition_id,
                                                   ServiceNode*& node, ProtocolType protocol, LBPolicyType policy,
                                                   IDCType idc, uint32_t locating_signal)
{
    auto cluster = getTopoCluster(topo_cluster_name);
    if (cluster == NULL) {
        return -1;
    }
    return cluster->allocNodeByPartitionId(node, partition_id, protocol, policy, idc, locating_signal);
}

int32_t TopoClusterManager::allocValidNodeByPartitionId(const char* topo_cluster_name, int32_t partition_id,
                                                        ServiceNode*& node, ProtocolType protocol, LBPolicyType policy,
                                                        IDCType idc, uint32_t locating_signal)
{
    auto cluster = getTopoCluster(topo_cluster_name);
    if (cluster == NULL) {
        return -1;
    }
    return cluster->allocValidNodeByPartitionId(node, partition_id, protocol, policy, idc, locating_signal);
}

int32_t TopoClusterManager::allocValidNodeByPartitionIdUint64(const char* topo_cluster_name, int32_t partition_id,
                                                              ServiceNode*& node, ProtocolType protocol,
                                                              LBPolicyType policy, IDCType idc,
                                                              uint64_t locating_signal)
{
    auto cluster = getTopoCluster(topo_cluster_name);
    if (cluster == NULL) {
        return -1;
    }
    return cluster->allocValidNodeByPartitionIdUint64(node, partition_id, protocol, policy, idc, locating_signal);
}

int32_t TopoClusterManager::allocAllNode(const char* topo_cluster_name, std::vector<ServiceNode*>& node_vec, bool valid)
{
    if (topo_cluster_name == NULL) {
        return -1;
    }

    auto cluster = getTopoCluster(topo_cluster_name);
    if (cluster == NULL) {
        return -1;
    }

    return cluster->allocAllNode(node_vec, valid);
}

size_t TopoClusterManager::getQueriedClusters(std::vector<std::string>& names)
{
#ifdef NDEBUG
    const int64_t interval = 30 * 60 * 1000 * 1000; // 30 min
#else
    const int64_t interval = 2 * 1000 * 1000; // for unittest, 2 sec
#endif
    int64_t now = autil::TimeUtility::currentTime();
    autil::ScopedLock lock(_queriedMutex);
    for (QueriedClusters::iterator it = _queriedClusters.begin(); it != _queriedClusters.end();) {
        if (it->second + interval < now) {
            _queriedClusters.erase(it++);
        } else {
            names.push_back((it++)->first);
        }
    }
    return names.size();
}

} // namespace cm_sub
