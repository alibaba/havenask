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
#include "aios/apps/facility/cm2/cm_sub/lb_policy/local_idc_prio_round_robin_policy.h"

using namespace cm_basic;

namespace cm_sub {

bool LocalIDCPrioRoundRobinPolicy::init(const std::shared_ptr<TopoPartition>& topoPartition)
{
    _rrPolicy = std::make_shared<RoundRobinPolicy>();
    return _rrPolicy->init(topoPartition);
}

ServiceNode* LocalIDCPrioRoundRobinPolicy::getServiceNode(const std::shared_ptr<TopoPartition>& topoPartition,
                                                          ProtocolType protocol, IDCType idc, uint32_t locatingSignal)
{
    if (!_rrPolicy || !topoPartition || topoPartition->getNodeSize() == 0) {
        return nullptr;
    }
    if (topoPartition->isCloseIDCPreferNew() || idc == cm_basic::IDC_ANY) {
        return _rrPolicy->getServiceNode(topoPartition, protocol, cm_basic::IDC_ANY, locatingSignal);
    }

    return _rrPolicy->getServiceNode(topoPartition, protocol, idc, locatingSignal);
}

ServiceNode* LocalIDCPrioRoundRobinPolicy::getValidServiceNode(const std::shared_ptr<TopoPartition>& topoPartition,
                                                               ProtocolType protocol, IDCType idc,
                                                               uint32_t locatingSignal)
{
    if (!_rrPolicy || !topoPartition || topoPartition->getNodeSize() == 0) {
        return nullptr;
    }
    if (topoPartition->isCloseIDCPreferNew() || idc == cm_basic::IDC_ANY) {
        return _rrPolicy->getValidServiceNode(topoPartition, protocol, cm_basic::IDC_ANY, locatingSignal);
    }

    autil::ScopedWriteLock lock(_lock);
    if (_idcCurId.find((int32_t)idc) == _idcCurId.end()) {
        // only set a random initial value
        _idcCurId[(int32_t)idc] = _rrPolicy->getCurId();
    }

    int32_t nodeId = 0;
    int32_t validWeight = 0;
    int32_t totalWeight = 0;
    topoPartition->getIDCNodeWeight(validWeight, totalWeight, protocol, idc);

    if (validWeight <= 0) {
        return _rrPolicy->getValidServiceNode(topoPartition, protocol, cm_basic::IDC_ANY, locatingSignal);
    }

    int32_t idcPreferRatioNew = topoPartition->getIDCPreferRatioNew();
    // `idcPreferRatioNew` can be 0, if it's 0 and `validWeight` = 0 either, we don't use
    // these nodes in other IDCs.
    if (validWeight * 100 >= totalWeight * idcPreferRatioNew) {
        nodeId = topoPartition->getNextValidNodeId(protocol, idc, _idcCurId[(int32_t)idc]);
        if (nodeId < 0) {
            return nullptr;
        }
        _idcCurId[(int32_t)idc] = nodeId;
        return new (std::nothrow) ServiceNode(topoPartition->getNodeByIndex(nodeId), protocol, cm_basic::NS_NORMAL);
    } else {
        // these normal nodes serve same count requests
        nodeId = topoPartition->getNextNodeId(protocol, idc, _idcCurId[(int32_t)idc]);
        if (nodeId < 0) {
            return nullptr;
        }

        _idcCurId[(int32_t)idc] = nodeId;
        if (topoPartition->getNodeByIndex(nodeId)->getNodeStatus() != NS_NORMAL) {
            // only round-robin these abnormal nodes' requests
            return _rrPolicy->getValidServiceNode(topoPartition, protocol, cm_basic::IDC_ANY, locatingSignal);
        } else {
            return new (std::nothrow) ServiceNode(topoPartition->getNodeByIndex(nodeId), protocol, cm_basic::NS_NORMAL);
        }
    }

    return nullptr;
}

} // namespace cm_sub
