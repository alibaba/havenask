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
#include "aios/apps/facility/cm2/cm_sub/lb_policy/conhash_policy.h"

using namespace cm_basic;

namespace cm_sub {

bool ConhashPolicy::init(const std::shared_ptr<TopoPartition>& topoPartition)
{
    autil::ScopedWriteLock lock(_lock);
    _conhash = std::make_shared<JumpWeightConHash>();
    if (!topoPartition) {
        return true;
    }

    auto nodes = topoPartition->allocAllNode(true);
    for (auto& node : nodes) {
        _conhash->addNode(node->getNodeSpec(), node->getConhashReplica(), node->getVirtualNodeRatio());
    }
    return true;
}

bool ConhashPolicy::inherit(const std::shared_ptr<ILBPolicy>& originPolicy,
                            const std::shared_ptr<TopoPartition>& originPartition,
                            const std::shared_ptr<TopoPartition>& newPartition)
{
    auto originConhashPolicy = std::dynamic_pointer_cast<ConhashPolicy>(originPolicy);
    if (!originConhashPolicy || !originConhashPolicy->_conhash || !originPartition || !newPartition) {
        return false;
    }

    autil::ScopedWriteLock lock(_lock);
    _conhash = std::make_shared<JumpWeightConHash>();
    if (!_conhash) {
        return false;
    }
    //_conhash = originConhashPolicy->_conhash;
    // TODO: clone cost a lot of time, maybe we can reuse conhash
    _conhash->clone(originConhashPolicy->_conhash);

    // 全量节点diff, 检查全量旧节点.
    auto originNodes = originPartition->allocAllNode(false);
    for (auto& originNode : originNodes) {
        auto newNode = newPartition->getNodeBySpec(originNode->getNodeSpec());
        if (newNode == nullptr || newNode->getNodeStatus() != NS_NORMAL) {
            // new node not match, should delete
            _conhash->updateNode(originNode->getNodeSpec(), 0, 0);
            continue;
        }
    }

    auto newNodes = newPartition->allocAllNode(false);
    for (auto newNode : newNodes) {
        if (newNode->getNodeStatus() != NS_NORMAL) {
            // node should not be lookup
            continue;
        }
        _conhash->updateNode(newNode->getNodeSpec(), newNode->getConhashReplica(), newNode->getVirtualNodeRatio());
    }
    return true;
}

ServiceNode* ConhashPolicy::getServiceNode(const std::shared_ptr<TopoPartition>& topoPartition, ProtocolType protocol,
                                           IDCType idc, uint32_t locatingSignal)
{
    if (locatingSignal <= 0 || !_conhash || !topoPartition || topoPartition->getNodeSize() == 0) {
        return nullptr;
    }

    std::string nodeId;
    {
        autil::ScopedReadLock lock(_lock);
        nodeId = _conhash->lookup(locatingSignal);
    }
    if (nodeId.empty()) {
        return nullptr;
    }

    auto node = topoPartition->getNodeBySpec(nodeId);
    if (node && node->isMatch(protocol, idc)) {
        return new (std::nothrow) ServiceNode(node, protocol);
    }
    return nullptr;
}

ServiceNode* ConhashPolicy::getValidServiceNode(const std::shared_ptr<TopoPartition>& topoPartition,
                                                ProtocolType protocol, IDCType idc, uint32_t locatingSignal)
{
    return getValidServiceNode(topoPartition, protocol, idc, (uint64_t)locatingSignal);
}

ServiceNode* ConhashPolicy::getValidServiceNode(const std::shared_ptr<TopoPartition>& topoPartition,
                                                ProtocolType protocol, IDCType idc, uint64_t locatingSignal)
{
    if (locatingSignal <= 0 || !_conhash || !topoPartition || topoPartition->getNodeSize() == 0) {
        return nullptr;
    }

    int32_t nodeCount = topoPartition->getNodeSize();

    bool hasUpdate = false;
    {
        for (int32_t i = 0; i < nodeCount; ++i) {
            if (likely(topoPartition->getNodeStatusByIndex(i) != topoPartition->getNodeByIndex(i)->getNodeStatus())) {
                hasUpdate = true;
                break;
            }
        }
        if (!hasUpdate) {
            std::string nodeId;
            {
                autil::ScopedReadLock lock(_lock);
                nodeId = _conhash->lookup(locatingSignal);
            }
            if (nodeId.empty()) {
                return nullptr;
            }

            auto node = topoPartition->getNodeBySpec(nodeId);
            if (node && node->isMatchValid(protocol, idc)) {
                return new (std::nothrow) ServiceNode(node, protocol, cm_basic::NS_NORMAL);
            }
            return nullptr;
        }
    }

    autil::ScopedWriteLock lock(_lock);
    for (int32_t i = 0; i < nodeCount; ++i) {
        auto node = topoPartition->getNodeByIndex(i);
        auto nodeStatus = node->getNodeStatus();
        auto curStatus = topoPartition->getNodeStatusByIndex(i);
        std::string spec = node->getNodeSpec();
        if (unlikely(curStatus != nodeStatus)) {
            topoPartition->setNodeStatusByIndex(i, nodeStatus);
            if (nodeStatus == cm_basic::NS_NORMAL) {
                _conhash->addNode(spec, node->getConhashReplica(), node->getVirtualNodeRatio());
            } else {
                _conhash->removeNode(spec, node->getConhashReplica(), node->getVirtualNodeRatio());
            }
            continue;
        }
        // after set node status, check weight
        if (unlikely(topoPartition->getNodeStatusByIndex(i) == nodeStatus) &&
            topoPartition->getNodeStatusByIndex(i) == cm_basic::NS_ABNORMAL) {
            continue;
        }

        int currentWeight = _conhash->getWeight(spec);
        if (currentWeight == 0) {
            continue;
        }
        int newWeight = node->getConhashReplica();
        if (unlikely(newWeight != currentWeight)) {
            int changeWeight = newWeight - currentWeight;
            if (changeWeight > 0) {
                _conhash->addNode(spec, changeWeight);
            } else {
                _conhash->removeNode(spec, -changeWeight);
            }
        }
    }

    auto nodeId = _conhash->lookup(locatingSignal);
    if (nodeId.empty()) {
        return nullptr;
    }

    auto node = topoPartition->getNodeBySpec(nodeId);
    if (node && node->isMatchValid(protocol, idc)) {
        return new (std::nothrow) ServiceNode(node, protocol, cm_basic::NS_NORMAL);
    }
    return nullptr;
}

std::shared_ptr<JumpWeightConHash> ConhashPolicy::getConhash()
{
    autil::ScopedReadLock lock(_lock);
    return _conhash;
}

} // namespace cm_sub