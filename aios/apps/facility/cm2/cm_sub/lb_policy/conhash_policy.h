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
#ifndef __CM_SUB_CONHASH_POLICY_H_
#define __CM_SUB_CONHASH_POLICY_H_

#include "aios/apps/facility/cm2/cm_sub/conhash/weight_conhash.h"
#include "aios/apps/facility/cm2/cm_sub/lb_policy/lb_policy.h"

namespace cm_sub {

class ConhashPolicy : public ILBPolicy
{
public:
    ConhashPolicy() : ILBPolicy() {}
    ~ConhashPolicy() {}

public:
    bool init(const std::shared_ptr<TopoPartition>& partition) override;

    virtual bool inherit(const std::shared_ptr<ILBPolicy>& originPolicy,
                         const std::shared_ptr<TopoPartition>& originPartition,
                         const std::shared_ptr<TopoPartition>& newPartition) override;

    ServiceNode* getServiceNode(const std::shared_ptr<TopoPartition>& topoPartition, ProtocolType protocol, IDCType idc,
                                uint32_t locatingSignal) override;
    ServiceNode* getValidServiceNode(const std::shared_ptr<TopoPartition>& topoPartition, ProtocolType protocol,
                                     IDCType idc, uint32_t locatingSignal) override;
    ServiceNode* getValidServiceNode(const std::shared_ptr<TopoPartition>& topoPartition, ProtocolType protocol,
                                     IDCType idc, uint64_t locatingSignal) override;

public:
    // for test
    std::shared_ptr<JumpWeightConHash> getConhash();

private:
    autil::ReadWriteLock _lock;
    std::shared_ptr<JumpWeightConHash> _conhash;
};

} // namespace cm_sub

#endif // __CM_SUB_CONHASH_POLICY_H_