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
#ifndef __CM_SUB_LB_POLICY_TABLE_H_
#define __CM_SUB_LB_POLICY_TABLE_H_

#include "aios/apps/facility/cm2/cm_sub/lb_policy/lb_policy.h"

namespace cm_sub {

class LBPolicyTable
{
public:
    bool init(const std::shared_ptr<TopoPartition>& partition);
    bool inherit(const std::shared_ptr<LBPolicyTable>& originTable,
                 const std::shared_ptr<TopoPartition>& originPartition,
                 const std::shared_ptr<TopoPartition>& newPartition);
    std::shared_ptr<ILBPolicy> getPolicy(LBPolicyType type);

public:
    static std::shared_ptr<LBPolicyTable> create(const std::shared_ptr<TopoPartition>& partition);
    static std::shared_ptr<LBPolicyTable> create(const std::shared_ptr<LBPolicyTable>& originTable,
                                                 const std::shared_ptr<TopoPartition>& originPartition,
                                                 const std::shared_ptr<TopoPartition>& newPartition);

private:
    std::vector<std::shared_ptr<ILBPolicy>> _lbPolicy;
};

} // namespace cm_sub

#endif