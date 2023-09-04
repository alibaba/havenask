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
#include "aios/apps/facility/cm2/cm_sub/lb_policy/weight_round_robin_policy.h"

using namespace cm_basic;

namespace cm_sub {

bool WeightRoundRobinPolicy::init(const std::shared_ptr<TopoPartition>& partition)
{
    if (!partition) {
        return false;
    }

    _wrr = std::make_shared<WeightedRoundRobin>();
    auto nodeWeights = partition->getNodeWeights();
    for (auto& nodeWeight : nodeWeights) {
        _wrr->addNode(nodeWeight);
    }
    _wrr->initSchedData();

    srand(autil::TimeUtility::currentTimeInMicroSeconds());
    if (nodeWeights.size() > 0) {
        _wrr->setCurrentId(rand() % nodeWeights.size());
    }

    _rrPolicy = std::make_shared<RoundRobinPolicy>();
    return _rrPolicy->init(partition);
}

ServiceNode* WeightRoundRobinPolicy::getServiceNode(const std::shared_ptr<TopoPartition>& topoPartition,
                                                    ProtocolType protocol, IDCType idc, uint32_t locatingSignal)
{
    auto matchFunc = [topoPartition, protocol, idc](int id) -> bool {
        auto node = topoPartition->getNodeByIndex(id);
        return node && node->isMatch(protocol, idc);
    };

    autil::ScopedWriteLock lock(_lock);
    if (!_wrr) {
        return nullptr;
    }

    int32_t nodeId = _wrr->schedule(matchFunc);
    if (nodeId != -1) {
        return new (std::nothrow) ServiceNode(topoPartition->getNodeByIndex(nodeId), protocol);
    }

    if (idc != cm_basic::IDC_ANY && _rrPolicy) {
        return _rrPolicy->getServiceNode(topoPartition, protocol, idc, locatingSignal);
    }
    return nullptr;
}

ServiceNode* WeightRoundRobinPolicy::getValidServiceNode(const std::shared_ptr<TopoPartition>& topoPartition,
                                                         ProtocolType protocol, IDCType idc, uint32_t locatingSignal)
{
    auto matchFunc = [topoPartition, protocol, idc](int id) -> bool {
        auto node = topoPartition->getNodeByIndex(id);
        return node && node->isMatchValid(protocol, idc);
    };

    autil::ScopedWriteLock lock(_lock);
    if (!_wrr) {
        return nullptr;
    }

    int32_t nodeId = _wrr->schedule(matchFunc);
    if (nodeId != -1) {
        return new (std::nothrow) ServiceNode(topoPartition->getNodeByIndex(nodeId), protocol, cm_basic::NS_NORMAL);
    }

    if (idc != cm_basic::IDC_ANY && _rrPolicy) {
        return _rrPolicy->getValidServiceNode(topoPartition, protocol, cm_basic::IDC_ANY, locatingSignal);
    }
    return nullptr;
}

} // namespace cm_sub