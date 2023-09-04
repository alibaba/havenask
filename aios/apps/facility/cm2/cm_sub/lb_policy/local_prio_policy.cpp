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
#include "aios/apps/facility/cm2/cm_sub/lb_policy/local_prio_policy.h"

#include "autil/NetUtil.h"

using namespace cm_basic;

namespace cm_sub {

LocalPrioPolicy::LocalPrioPolicy() : ILBPolicy() { autil::NetUtil::GetDefaultIp(_localIp); }

bool LocalPrioPolicy::init(const std::shared_ptr<TopoPartition>& partition)
{
    _rrPolicy = std::make_shared<RoundRobinPolicy>();
    return _rrPolicy->init(partition);
}

ServiceNode* LocalPrioPolicy::getServiceNode(const std::shared_ptr<TopoPartition>& topoPartition, ProtocolType protocol,
                                             IDCType idc, uint32_t locatingSignal)
{
    if (!_rrPolicy || !topoPartition || topoPartition->getNodeSize() == 0) {
        return nullptr;
    }
    auto topoNode = topoPartition->getNodeByIndex(_rrPolicy->getCurId() % topoPartition->getNodeSize());
    if (topoNode->getNodeIP() == _localIp && topoNode->isMatch(protocol, idc)) {
        return new (std::nothrow) ServiceNode(topoNode, protocol);
    }
    ServiceNode* node = _rrPolicy->getServiceNode(topoPartition, protocol, idc, locatingSignal);
    if (node == nullptr && idc != cm_basic::IDC_ANY) {
        return _rrPolicy->getServiceNode(topoPartition, protocol, cm_basic::IDC_ANY, locatingSignal);
    }
    return node;
}

ServiceNode* LocalPrioPolicy::getValidServiceNode(const std::shared_ptr<TopoPartition>& topoPartition,
                                                  ProtocolType protocol, IDCType idc, uint32_t locatingSignal)
{
    if (!_rrPolicy || !topoPartition || topoPartition->getNodeSize() == 0) {
        return nullptr;
    }
    auto topoNode = topoPartition->getNodeByIndex(_rrPolicy->getCurId() % topoPartition->getNodeSize());
    if (topoNode->getNodeIP() == _localIp && topoNode->isMatchValid(protocol, idc)) {
        return new (std::nothrow) ServiceNode(topoNode, protocol, cm_basic::NS_NORMAL);
    }
    ServiceNode* node = _rrPolicy->getValidServiceNode(topoPartition, protocol, idc, locatingSignal);
    if (node == nullptr && idc != cm_basic::IDC_ANY) {
        return _rrPolicy->getValidServiceNode(topoPartition, protocol, cm_basic::IDC_ANY, locatingSignal);
    }
    return node;
}

} // namespace cm_sub