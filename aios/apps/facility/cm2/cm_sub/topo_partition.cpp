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
#include "aios/apps/facility/cm2/cm_sub/topo_partition.h"

using namespace cm_basic;

namespace cm_sub {

AUTIL_LOG_SETUP(cm_sub, TopoPartition);

TopoPartition::TopoPartition() : _idcPreferRatioNew(0), _closeIdcPreferNew(true) {}

TopoPartition::TopoPartition(const std::shared_ptr<TopoPartition>& t)
{
    std::map<std::shared_ptr<TopoNode>, std::shared_ptr<TopoNode>> nodeMap;
    for (uint32_t i = 0; i < t->_nodeVec.size(); ++i) {
        auto n = std::make_shared<TopoNode>((t->_nodeVec)[i]);
        _nodeVec.push_back(n);
        _nodeStatusVec.push_back(n->getNodeStatus());
        nodeMap[t->_nodeVec[i]] = n;
    }

    for (auto& iter : t->_spec2Nodes) {
        _spec2Nodes[iter.first] = nodeMap[iter.second];
    }
    _idcPreferRatioNew = t->_idcPreferRatioNew;
    _closeIdcPreferNew = t->_closeIdcPreferNew;
}

TopoPartition::~TopoPartition() { _nodeVec.clear(); }

void TopoPartition::addNode(const std::shared_ptr<TopoNode>& node)
{
    if (node == nullptr) {
        return;
    }
    autil::ScopedWriteLock lock(_rwLock);
    _nodeVec.push_back(node);
}

size_t TopoPartition::getNodeSize()
{
    autil::ScopedReadLock lock(_rwLock);
    return _nodeVec.size();
}

size_t TopoPartition::getValidNodeSize()
{
    size_t nodeCount = 0;

    autil::ScopedReadLock lock(_rwLock);
    for (auto& node : _nodeVec) {
        if (node->getNodeStatus() == cm_basic::NS_NORMAL) {
            ++nodeCount;
        }
    }
    return nodeCount;
}

double TopoPartition::getValidNodeRatio()
{
    auto nodeCount = getNodeSize();
    return nodeCount == 0 ? 0 : double(getValidNodeSize()) / double(nodeCount);
}

std::shared_ptr<TopoNode> TopoPartition::getNodeByIndex(size_t i)
{
    autil::ScopedReadLock lock(_rwLock);
    if (i >= _nodeVec.size()) {
        return nullptr;
    }
    return _nodeVec[i];
}

std::shared_ptr<TopoNode> TopoPartition::getNodeBySpec(const std::string& spec)
{
    autil::ScopedReadLock lock(_rwLock);
    auto iter = _spec2Nodes.find(spec);
    if (iter == _spec2Nodes.end()) {
        return nullptr;
    }
    return iter->second;
}

cm_basic::NodeStatus TopoPartition::getNodeStatusByIndex(size_t i)
{
    autil::ScopedReadLock lock(_rwLock);
    if (i >= _nodeStatusVec.size()) {
        return cm_basic::NS_UNVALID;
    }
    return _nodeStatusVec[i];
}

void TopoPartition::setNodeStatusByIndex(size_t i, cm_basic::NodeStatus status)
{
    autil::ScopedWriteLock lock(_rwLock);
    if (i >= _nodeStatusVec.size()) {
        return;
    }
    _nodeStatusVec[i] = status;
}

std::vector<std::shared_ptr<TopoNode>> TopoPartition::allocAllNode(bool valid)
{
    autil::ScopedReadLock lock(_rwLock);
    if (!valid) {
        return _nodeVec;
    }
    std::vector<std::shared_ptr<TopoNode>> nodes;
    for (auto& node : _nodeVec) {
        if (node->getNodeStatus() == NS_NORMAL) {
            nodes.push_back(node);
        }
    }
    return nodes;
}

std::vector<std::shared_ptr<TopoNode>> TopoPartition::allocNode(ProtocolType protocol, IDCType idc)
{
    std::vector<std::shared_ptr<TopoNode>> nodes;

    autil::ScopedReadLock lock(_rwLock);
    for (auto& node : _nodeVec) {
        if (node->isMatch(protocol, idc)) {
            nodes.push_back(node);
        }
    }
    return nodes;
}

std::vector<std::shared_ptr<TopoNode>> TopoPartition::allocValidNode(ProtocolType protocol, IDCType idc)
{
    std::vector<std::shared_ptr<TopoNode>> nodes;

    autil::ScopedReadLock lock(_rwLock);
    for (auto& node : _nodeVec) {
        if (node->isMatchValid(protocol, idc)) {
            nodes.push_back(node);
        }
    }
    return nodes;
}

void TopoPartition::initLBPolicyInfo()
{
    autil::ScopedWriteLock lock(_rwLock);
    _spec2Nodes.clear();
    _nodeStatusVec.clear();
    for (auto& node : _nodeVec) {
        // 初始化 节点权重
        int32_t weight = DefaultLBWeight;
        if (node->hasValidLBInfoWeight()) {
            weight = node->getLBInfoWeight();
        } else {
            node->setLBInfoWeight(weight);
        }

        std::string spec = node->getNodeSpec();
        _nodeStatusVec.push_back(node->getNodeStatus());
        _spec2Nodes[spec] = node;
    }
    assert(_nodeStatusVec.size() == _nodeVec.size());
}

void TopoPartition::merge(const std::string& clusterName, const std::shared_ptr<TopoPartition>& from)
{
    autil::ScopedWriteLock lock(_rwLock);
    for (auto& fromNode : from->_nodeVec) {
        if (fromNode->getClusterName() != clusterName) {
            auto node = std::make_shared<TopoNode>(fromNode);
            _nodeVec.push_back(node);
        }
    }
}

std::shared_ptr<TopoPartition> TopoPartition::split(const std::string& clusterName)
{
    autil::ScopedReadLock lock(_rwLock);
    auto newPartition = std::make_shared<TopoPartition>();
    newPartition->_idcPreferRatioNew = _idcPreferRatioNew;
    newPartition->_closeIdcPreferNew = _closeIdcPreferNew;
    for (auto& node : _nodeVec) {
        if (node->getClusterName() != clusterName) {
            auto newNode = std::make_shared<TopoNode>(node);
            newPartition->_nodeVec.push_back(newNode);
        }
    }
    return newPartition;
}

void TopoPartition::printTopo()
{
    autil::ScopedReadLock lock(_rwLock);
    for (auto& node : _nodeVec) {
        auto cmNode = node->getNode();
        AUTIL_LOG(INFO, "---------- node_ip:%s ----------", cmNode->getNodeIP().c_str());
        AUTIL_LOG(INFO, "cluster_name:%s, group_id:%d, cur_status:%d", cmNode->getClusterName().c_str(),
                  cmNode->getGroupID(), cmNode->getCurStatus());
    }
}

std::vector<int> TopoPartition::getNodeWeights()
{
    std::vector<int> weights;
    autil::ScopedReadLock lock(_rwLock);
    weights.reserve(_nodeVec.size());
    for (auto& node : _nodeVec) {
        weights.push_back(node->getWeight());
    }
    return weights;
}

void TopoPartition::getIDCNodeWeight(int32_t& validWeight, int32_t& totalWeight, ProtocolType protocol, IDCType idc)
{
    validWeight = 0;
    totalWeight = 0;

    autil::ScopedReadLock lock(_rwLock);
    for (auto& node : _nodeVec) {
        if (!(node->isMatch(protocol, idc))) {
            continue;
        }

        int32_t nodeWeight = TopoPartition::DefaultLBWeight;
        if (node->hasValidLBInfoWeight()) {
            nodeWeight = node->getLBInfoWeight();
        }

        totalWeight += nodeWeight;
        if (node->getNodeStatus() == NS_NORMAL) {
            validWeight += nodeWeight;
        }
    }
}

int32_t TopoPartition::getNextNodeId(ProtocolType protocol, IDCType idc, int32_t nid)
{
    if (nid < 0) {
        nid = 0;
    }

    autil::ScopedReadLock lock(_rwLock);
    int32_t nodeCount = _nodeVec.size();
    for (int32_t i = 0; i < nodeCount; i++) {
        nid = (nid + 1) % nodeCount;
        if (_nodeVec[nid]->isMatch(protocol, idc)) {
            return nid;
        }
    }
    return -1;
}

int32_t TopoPartition::getNextValidNodeId(ProtocolType protocol, IDCType idc, int32_t nid)
{
    if (nid < 0) {
        nid = 0;
    }

    autil::ScopedReadLock lock(_rwLock);
    int32_t nodeCount = _nodeVec.size();
    for (int32_t i = 0; i < nodeCount; i++) {
        nid = (nid + 1) % nodeCount;
        if (_nodeVec[nid]->isMatchValid(protocol, idc)) {
            return nid;
        }
    }
    return -1;
}

} // namespace cm_sub