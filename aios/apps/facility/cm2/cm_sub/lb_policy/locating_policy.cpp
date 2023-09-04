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
#include "aios/apps/facility/cm2/cm_sub/lb_policy/locating_policy.h"

using namespace cm_basic;

namespace cm_sub {

bool LocatingPolicy::init(const std::shared_ptr<TopoPartition>& partition)
{
    _rrPolicy = std::make_shared<RoundRobinPolicy>();
    return _rrPolicy->init(partition);
}

ServiceNode* LocatingPolicy::getServiceNode(const std::shared_ptr<TopoPartition>& topoPartition, ProtocolType protocol,
                                            IDCType idc, uint32_t locatingSignal)
{
    if (locatingSignal <= 0 || !topoPartition || topoPartition->getNodeSize() == 0) {
        return nullptr;
    }

    int32_t nodeCount = topoPartition->getNodeSize();
    int32_t nodeId = locatingSignal % nodeCount;
    auto node = topoPartition->getNodeByIndex(nodeId);
    if (node && node->isMatch(protocol, idc)) {
        return new (std::nothrow) ServiceNode(node, protocol);
    }

    // 如果指定该行的节点无效，那么按轮询的方式
    if (_rrPolicy) {
        return _rrPolicy->getServiceNode(topoPartition, protocol, cm_basic::IDC_ANY, locatingSignal);
    }
    return nullptr;
}

ServiceNode* LocatingPolicy::getValidServiceNode(const std::shared_ptr<TopoPartition>& topoPartition,
                                                 ProtocolType protocol, IDCType idc, uint32_t locatingSignal)
{
    if (locatingSignal <= 0 || !topoPartition || topoPartition->getNodeSize() == 0) {
        return nullptr;
    }

    int32_t nodeCount = topoPartition->getNodeSize();
    int32_t nodeId = locatingSignal % nodeCount;
    auto node = topoPartition->getNodeByIndex(nodeId);
    if (node && node->isMatchValid(protocol, idc)) {
        return new (std::nothrow) ServiceNode(node, protocol, cm_basic::NS_NORMAL);
    }
    // 如果指定该行的节点无效，那么按轮询的方式
    if (_rrPolicy) {
        return _rrPolicy->getValidServiceNode(topoPartition, protocol, cm_basic::IDC_ANY, locatingSignal);
    }
    return nullptr;
}

} // namespace cm_sub