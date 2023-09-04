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
#include "aios/apps/facility/cm2/cm_sub/lb_policy/local_idc_prio_hash_policy.h"

using namespace cm_basic;

namespace cm_sub {

bool LocalIDCPrioHashPolicy::init(const std::shared_ptr<TopoPartition>& topoPartition)
{
    _rrPolicy = std::make_shared<RoundRobinPolicy>();
    return _rrPolicy->init(topoPartition);
}

ServiceNode* LocalIDCPrioHashPolicy::getServiceNode(const std::shared_ptr<TopoPartition>& topoPartition,
                                                    ProtocolType protocol, IDCType idc, uint32_t locatingSignal)
{
    if (locatingSignal <= 0 || !_rrPolicy || !topoPartition || topoPartition->getNodeSize() == 0) {
        return nullptr;
    }

    autil::ScopedWriteLock lock(_lock);

    if (topoPartition->isCloseIDCPreferNew() || idc == cm_basic::IDC_ANY) {
        return _rrPolicy->getServiceNode(topoPartition, protocol, cm_basic::IDC_ANY, locatingSignal);
    }

    auto nodes = topoPartition->allocNode(protocol, idc);
    if (nodes.empty()) {
        return _rrPolicy->getServiceNode(topoPartition, protocol, cm_basic::IDC_ANY, locatingSignal);
    }

    auto& node = nodes[locatingSignal % nodes.size()];
    return new (std::nothrow) ServiceNode(node, protocol);
}

ServiceNode* LocalIDCPrioHashPolicy::getValidServiceNode(const std::shared_ptr<TopoPartition>& topoPartition,
                                                         ProtocolType protocol, IDCType idc, uint32_t locatingSignal)
{
    if (locatingSignal <= 0 || !_rrPolicy || !topoPartition || topoPartition->getNodeSize() == 0) {
        return nullptr;
    }

    autil::ScopedWriteLock lock(_lock);

    if (topoPartition->isCloseIDCPreferNew() || idc == cm_basic::IDC_ANY) {
        return _rrPolicy->getValidServiceNode(topoPartition, protocol, cm_basic::IDC_ANY, locatingSignal);
    }

    if (_idcCurId.find((int32_t)idc) == _idcCurId.end()) {
        _idcCurId[(int32_t)idc] = _rrPolicy->getCurId();
    }

    auto nodes = topoPartition->allocNode(protocol, idc);
    if (nodes.empty()) {
        return _rrPolicy->getValidServiceNode(topoPartition, protocol, cm_basic::IDC_ANY, locatingSignal);
    }

    auto& node = nodes[locatingSignal % nodes.size()];
    if (node->getNodeStatus() == NS_NORMAL) {
        return new (std::nothrow) ServiceNode(node, protocol, cm_basic::NS_NORMAL);
    }

    int32_t validWeight = 0;
    int32_t totalWeight = 0;
    topoPartition->getIDCNodeWeight(validWeight, totalWeight, protocol, idc);

    int32_t idcPreferRatioNew = topoPartition->getIDCPreferRatioNew();
    if (validWeight * 100 >= totalWeight * idcPreferRatioNew) {
        auto nodeId = topoPartition->getNextValidNodeId(protocol, idc, _idcCurId[(int32_t)idc]);
        if (nodeId < 0) {
            return nullptr;
        }
        _idcCurId[(int32_t)idc] = nodeId;
        return new (std::nothrow) ServiceNode(topoPartition->getNodeByIndex(nodeId), protocol, cm_basic::NS_NORMAL);
    } else {
        return _rrPolicy->getValidServiceNode(topoPartition, protocol, cm_basic::IDC_ANY, locatingSignal);
    }

    return NULL;
}

} // namespace cm_sub