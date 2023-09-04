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
#include "aios/apps/facility/cm2/cm_sub/query_interface.h"

#include "aios/apps/facility/cm2/cm_basic/basic_struct/cm_central_sub.h"
#include "aios/apps/facility/cm2/cm_sub/topo_cluster_manager.h"

namespace cm_sub {

using namespace cm_basic;

void QueryInterface::freeNode(ServiceNode*& node) { _topoClusterManager->freeNode(node); }

void QueryInterface::freeNodes(std::vector<ServiceNode*>& nodes) { _topoClusterManager->freeNodes(nodes); }

int32_t QueryInterface::getPartitionCnt(const char* topo_cluster_name)
{
    return _topoClusterManager->getPartitionCnt(topo_cluster_name);
}

int32_t QueryInterface::getNodeCntOfPartition(const char* topo_cluster_name, int32_t partition_id)
{
    return _topoClusterManager->getNodeCntOfPartition(topo_cluster_name, partition_id);
}

int32_t QueryInterface::allocValidRow(std::vector<ServiceNode*>& node_vec, const char* topo_cluster_name,
                                      ProtocolType protocol, LBPolicyType policy, IDCType idc, uint32_t locating_signal)
{
    return _topoClusterManager->allocValidRow(topo_cluster_name, node_vec, protocol, policy, idc, locating_signal);
}

int32_t QueryInterface::allocValidVersionRow(std::vector<ServiceNode*>& node_vec, long& version,
                                             const char* topo_cluster_name, ProtocolType protocol, LBPolicyType policy,
                                             uint32_t locating_signal)
{
    return _topoClusterManager->allocValidVersionRow(node_vec, version, topo_cluster_name, protocol, policy,
                                                     locating_signal);
}

int32_t QueryInterface::allocRow(std::vector<ServiceNode*>& node_vec, const char* topo_cluster_name,
                                 ProtocolType protocol, LBPolicyType policy, IDCType idc, uint32_t locating_signal)
{
    return _topoClusterManager->allocRow(topo_cluster_name, node_vec, protocol, policy, idc, locating_signal);
}

int32_t QueryInterface::allocNodeOfPartition(std::vector<ServiceNode*>& node_vec, const char* topo_cluster_name,
                                             int32_t partition_id, ProtocolType protocol, IDCType idc)
{
    return _topoClusterManager->allocNodeOfPartition(topo_cluster_name, partition_id, node_vec, protocol, idc);
}

int32_t QueryInterface::allocValidNodeOfPartition(std::vector<ServiceNode*>& node_vec, const char* topo_cluster_name,
                                                  int32_t partition_id, ProtocolType protocol, IDCType idc)
{
    return _topoClusterManager->allocValidNodeOfPartition(topo_cluster_name, partition_id, node_vec, protocol, idc);
}

int32_t QueryInterface::allocValidNodeByPartitionId(ServiceNode*& node, const char* topo_cluster_name,
                                                    int32_t partition_id, ProtocolType protocol, LBPolicyType policy,
                                                    IDCType idc, uint32_t locating_signal)
{
    return _topoClusterManager->allocValidNodeByPartitionId(topo_cluster_name, partition_id, node, protocol, policy,
                                                            idc, locating_signal);
}

int32_t QueryInterface::allocValidNodeByPartitionIdUint64(ServiceNode*& node, const char* topo_cluster_name,
                                                          int32_t partition_id, ProtocolType protocol,
                                                          LBPolicyType policy, IDCType idc, uint64_t locating_signal)
{
    return _topoClusterManager->allocValidNodeByPartitionIdUint64(topo_cluster_name, partition_id, node, protocol,
                                                                  policy, idc, locating_signal);
}

int32_t QueryInterface::allocNodeByPartitionId(ServiceNode*& node, const char* topo_cluster_name, int32_t partition_id,
                                               ProtocolType protocol, LBPolicyType policy, IDCType idc,
                                               uint32_t locating_signal)
{
    return _topoClusterManager->allocNodeByPartitionId(topo_cluster_name, partition_id, node, protocol, policy, idc,
                                                       locating_signal);
}

int32_t QueryInterface::allocAllValidNode(std::vector<ServiceNode*>& node_vec, const char* topo_cluster_name)
{
    return _topoClusterManager->allocAllNode(topo_cluster_name, node_vec, true);
}

int32_t QueryInterface::allocAllNode(std::vector<ServiceNode*>& node_vec, const char* topo_cluster_name)
{
    return _topoClusterManager->allocAllNode(topo_cluster_name, node_vec, false);
}

int32_t QueryInterface::getNodeStatus(cm_basic::NodeStatus& node_status, const char* node_spec,
                                      const char* cluster_name)
{
    return _cmCentral->getNodeStatus(node_spec, cluster_name, node_status);
}

int32_t QueryInterface::getNodeMetaInfo(cm_basic::NodeMetaInfo* meta_info, const char* node_spec,
                                        const char* cluster_name)
{
    return _cmCentral->getNodeMetaInfo(node_spec, cluster_name, meta_info);
}

int32_t QueryInterface::getTopoClusterNames(const std::string& c_name, std::vector<std::string>& t_names)
{
    return _topoClusterManager->getTopoClusterNames(c_name, t_names);
}

} // namespace cm_sub
