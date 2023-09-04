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
#include "aios/apps/facility/cm2/cm_sub/lb_policy/random_policy.h"

using namespace cm_basic;

namespace cm_sub {

RandomPolicy::RandomPolicy() : ILBPolicy()
{
    // init seed for random generator
    timeval tval;
    gettimeofday(&tval, NULL);
    srand(tval.tv_usec);
}

int RandomPolicy::randnum(int max) const
{
    // high-order bits much more random than lower-order bits in some rand() implementations.
    // See rand(3) man-page's NOTES for more details
    return (int)((float)max * (rand() / (RAND_MAX + 1.0)));
}

ServiceNode* RandomPolicy::getServiceNode(const std::shared_ptr<TopoPartition>& topoPartition, ProtocolType protocol,
                                          IDCType idc, uint32_t locatingSignal)
{
    if (!topoPartition || topoPartition->getNodeSize() == 0) {
        return nullptr;
    }
    auto nodeVec = topoPartition->allocNode(protocol, idc);
    if (nodeVec.size() == 0 && idc != cm_basic::IDC_ANY) {
        nodeVec = topoPartition->allocNode(protocol, cm_basic::IDC_ANY);
    }

    // 从所有符合条件的节点中随机选取一个
    if (nodeVec.size() == 0) {
        return nullptr;
    }

    auto node = nodeVec[randnum(nodeVec.size())];
    return new (std::nothrow) ServiceNode(node, protocol);
}

ServiceNode* RandomPolicy::getValidServiceNode(const std::shared_ptr<TopoPartition>& topoPartition,
                                               ProtocolType protocol, IDCType idc, uint32_t locatingSignal)
{
    if (!topoPartition || topoPartition->getNodeSize() == 0) {
        return nullptr;
    }
    auto nodeVec = topoPartition->allocValidNode(protocol, idc);
    if (nodeVec.size() == 0 && idc != cm_basic::IDC_ANY) {
        nodeVec = topoPartition->allocValidNode(protocol, cm_basic::IDC_ANY);
    }

    // 从所有符合条件的节点中随机选取一个
    if (nodeVec.size() == 0) {
        return NULL;
    }
    auto node = nodeVec[randnum(nodeVec.size())];
    return new (std::nothrow) ServiceNode(node, protocol, cm_basic::NS_NORMAL);
}

} // namespace cm_sub