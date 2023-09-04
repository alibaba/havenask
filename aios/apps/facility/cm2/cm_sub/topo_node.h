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
#ifndef __CM_SUB_TOPO_NODE_H_
#define __CM_SUB_TOPO_NODE_H_

#include "aios/apps/facility/cm2/cm_basic/basic_struct/cm_node_wrapper.h"

namespace cm_sub {

// 集群结点信息, 包括: 结点状态信息, NodeMetaInfo
class TopoNode
{
public:
    TopoNode(cm_basic::CMNodeWrapperPtr node, int weightRatio, uint32_t virtualNodeRatio, long ver = 0,
             int tweight = -1)
        : _node(node)
        , _hashWeightRatio(weightRatio)
        , _virtualNodeRatio(virtualNodeRatio)
        , _version(ver)
        , _tWeight(tweight)
    {
    }
    TopoNode(const std::shared_ptr<TopoNode>& topoNode);
    ~TopoNode() {};

public:
    cm_basic::CMNodeWrapperPtr getNode() { return _node; }
    cm_basic::NodeStatus getNodeStatus() { return _node != nullptr ? _node->getCurStatus() : cm_basic::NS_UNVALID; }
    bool hasValidLBInfoWeight();
    int32_t getLBInfoWeight();
    void setLBInfoWeight(int32_t weight);
    std::string getNodeSpec() { return _node != nullptr ? _node->getNodeSpec() : ""; }
    std::string getNodeIP() { return _node != nullptr ? _node->getNodeIP() : ""; }
    std::string getClusterName() { return _node != nullptr ? _node->getClusterName() : ""; }

public:
    long getVersion() const { return _version; }
    bool isMatch(cm_basic::ProtocolType protocol, cm_basic::IDCType idc);
    bool isMatchValid(cm_basic::ProtocolType protocol, cm_basic::IDCType idc);

public:
    // 权重一致性哈希
    int getConhashReplica();
    int getWeight();
    uint32_t getVirtualNodeRatio() { return _virtualNodeRatio; }

private:
    cm_basic::CMNodeWrapperPtr _node;
    int _hashWeightRatio;
    uint32_t _virtualNodeRatio {1};
    long _version; // 扩展version，`cluster_map_table'中扩展字段
    int _tWeight;  // topo node 扩展权重
};

} // namespace cm_sub

#endif // __CM_SUB_TOPO_NODE_H_