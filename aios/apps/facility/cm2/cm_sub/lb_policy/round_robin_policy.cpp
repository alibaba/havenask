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
#include "aios/apps/facility/cm2/cm_sub/lb_policy/round_robin_policy.h"

using namespace cm_basic;

namespace cm_sub {

bool RoundRobinPolicy::init(const std::shared_ptr<TopoPartition>& partition)
{
    if (partition && partition->getNodeSize() > 0) {
        srand(autil::TimeUtility::currentTimeInMicroSeconds());
        _curId = rand() % partition->getNodeSize();
    }
    return true;
}

ServiceNode* RoundRobinPolicy::getServiceNode(const std::shared_ptr<TopoPartition>& topoPartition,
                                              ProtocolType protocol, IDCType idc, uint32_t locatingSignal)
{
    if (!topoPartition || topoPartition->getNodeSize() == 0) {
        return nullptr;
    }

    autil::ScopedWriteLock lock(_lock);
    auto id = topoPartition->getNextNodeId(protocol, idc, _curId);
    if (id == -1) {
        idc = cm_basic::IDC_ANY;
        id = topoPartition->getNextNodeId(protocol, idc, _curId);
    }
    if (id != -1) {
        _curId = id;
        auto node = topoPartition->getNodeByIndex(_curId);
        if (node && node->isMatch(protocol, idc)) {
            return new (std::nothrow) ServiceNode(node, protocol);
        }
    }
    return nullptr;
}

ServiceNode* RoundRobinPolicy::getValidServiceNode(const std::shared_ptr<TopoPartition>& topoPartition,
                                                   ProtocolType protocol, IDCType idc, uint32_t locatingSignal)
{
    if (!topoPartition || topoPartition->getNodeSize() == 0) {
        return nullptr;
    }

    autil::ScopedWriteLock lock(_lock);
    auto id = topoPartition->getNextValidNodeId(protocol, idc, _curId);
    if (id == -1) {
        idc = cm_basic::IDC_ANY;
        id = topoPartition->getNextValidNodeId(protocol, idc, _curId);
    }
    if (id != -1) {
        _curId = id;
        auto node = topoPartition->getNodeByIndex(_curId);
        if (node && node->isMatchValid(protocol, idc)) {
            return new (std::nothrow) ServiceNode(node, protocol);
        }
    }
    return nullptr;
}

int32_t RoundRobinPolicy::getCurId()
{
    autil::ScopedReadLock lock(_lock);
    return _curId;
}

} // namespace cm_sub