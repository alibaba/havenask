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

#ifndef __LB_POLICY_H_
#define __LB_POLICY_H_

#include <vector>

#include "aios/apps/facility/cm2/cm_basic/basic_struct/proto/cm_common.pb.h"
#include "aios/apps/facility/cm2/cm_sub/service_node.h"
#include "aios/apps/facility/cm2/cm_sub/topo_partition.h"

namespace cm_sub {

using namespace cm_basic;

/**
 * @enum LBPolicyType
 * @brief 获取的节点的负载均衡策略
 */
enum LBPolicyType {
    LB_ROUNDROBIN = 0,                // 轮询
    LB_RANDOM = 1,                    // 随机分配
    LB_WEIGHT = 2,                    // 根据权重
    LB_LOCATING = 3,                  // 指定选取哪一行
    LB_CONHASH = 4,                   // 指定行失败后，用一致性获取节点
    LB_LOCAL_PRIO = 5,                // 本机优先，如果本机不可用，则轮询
    LB_LOCAL_IDC_PRIO_ROUNDROBIN = 6, // 本机房优先，轮询
    LB_LOCAL_IDC_PRIO_HASH = 7        // 本机房优先，哈希
};

class ILBPolicy
{
public:
    ILBPolicy() {};
    virtual ~ILBPolicy() {};

public:
    virtual bool init(const std::shared_ptr<TopoPartition>& partition) = 0;
    virtual bool inherit(const std::shared_ptr<ILBPolicy>& originPolicy,
                         const std::shared_ptr<TopoPartition>& originPartition,
                         const std::shared_ptr<TopoPartition>& newPartition)
    {
        return false;
    }
    virtual ServiceNode* getServiceNode(const std::shared_ptr<TopoPartition>& topoPartition, ProtocolType protocol,
                                        IDCType idc, uint32_t locatingSignal) = 0;
    virtual ServiceNode* getValidServiceNode(const std::shared_ptr<TopoPartition>& topoPartition, ProtocolType protocol,
                                             IDCType idc, uint32_t locatingSignal) = 0;
    virtual ServiceNode* getValidServiceNode(const std::shared_ptr<TopoPartition>& topoPartition, ProtocolType protocol,
                                             IDCType idc, uint64_t locatingSignal)
    {
        return getValidServiceNode(topoPartition, protocol, idc, (uint32_t)locatingSignal);
    }
};

} // namespace cm_sub
#endif
