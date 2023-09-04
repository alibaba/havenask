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
#ifndef __CM_SUB_LOCAL_IDC_HASH_POLICY_H_
#define __CM_SUB_LOCAL_IDC_HASH_POLICY_H_

#include "aios/apps/facility/cm2/cm_sub/lb_policy/lb_policy.h"
#include "aios/apps/facility/cm2/cm_sub/lb_policy/round_robin_policy.h"

namespace cm_sub {

class LocalIDCPrioHashPolicy : public ILBPolicy
{
public:
    LocalIDCPrioHashPolicy() : ILBPolicy() {}
    ~LocalIDCPrioHashPolicy() {}

public:
    bool init(const std::shared_ptr<TopoPartition>& topoPartition) override;
    ServiceNode* getServiceNode(const std::shared_ptr<TopoPartition>& topoPartition, ProtocolType protocol, IDCType idc,
                                uint32_t locatingSignal) override;
    ServiceNode* getValidServiceNode(const std::shared_ptr<TopoPartition>& topoPartition, ProtocolType protocol,
                                     IDCType idc, uint32_t locatingSignal) override;

public:
    // for test
    const std::shared_ptr<RoundRobinPolicy>& getRRPolicy() { return _rrPolicy; }

private:
    autil::ReadWriteLock _lock;
    std::map<int32_t, int32_t> _idcCurId;
    std::shared_ptr<RoundRobinPolicy> _rrPolicy;
};

} // namespace cm_sub

#endif // __CM_SUB_LOCAL_IDC_HASH_POLICY_H_