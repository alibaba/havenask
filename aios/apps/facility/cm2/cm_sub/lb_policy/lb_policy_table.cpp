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
#include "aios/apps/facility/cm2/cm_sub/lb_policy/lb_policy_table.h"

#include "aios/apps/facility/cm2/cm_sub/lb_policy/conhash_policy.h"
#include "aios/apps/facility/cm2/cm_sub/lb_policy/local_idc_prio_hash_policy.h"
#include "aios/apps/facility/cm2/cm_sub/lb_policy/local_idc_prio_round_robin_policy.h"
#include "aios/apps/facility/cm2/cm_sub/lb_policy/local_prio_policy.h"
#include "aios/apps/facility/cm2/cm_sub/lb_policy/locating_policy.h"
#include "aios/apps/facility/cm2/cm_sub/lb_policy/random_policy.h"
#include "aios/apps/facility/cm2/cm_sub/lb_policy/round_robin_policy.h"
#include "aios/apps/facility/cm2/cm_sub/lb_policy/weight_round_robin_policy.h"

namespace cm_sub {

using namespace cm_basic;

bool LBPolicyTable::init(const std::shared_ptr<TopoPartition>& partition)
{
    _lbPolicy.resize(8);
    std::shared_ptr<ILBPolicy> policy;

#define REGIST_POLICY(policyClass, policyType)                                                                         \
    policy.reset(new policyClass);                                                                                     \
    if (!policy || !policy->init(partition)) {                                                                         \
        return false;                                                                                                  \
    }                                                                                                                  \
    _lbPolicy[policyType] = policy;

    REGIST_POLICY(RoundRobinPolicy, LB_ROUNDROBIN);
    REGIST_POLICY(RandomPolicy, LB_RANDOM);
    REGIST_POLICY(WeightRoundRobinPolicy, LB_WEIGHT);
    REGIST_POLICY(LocatingPolicy, LB_LOCATING);
    REGIST_POLICY(ConhashPolicy, LB_CONHASH);
    REGIST_POLICY(LocalPrioPolicy, LB_LOCAL_PRIO);
    REGIST_POLICY(LocalIDCPrioRoundRobinPolicy, LB_LOCAL_IDC_PRIO_ROUNDROBIN);
    REGIST_POLICY(LocalIDCPrioHashPolicy, LB_LOCAL_IDC_PRIO_HASH);

#undef REGIST_POLICY
    return true;
}

bool LBPolicyTable::inherit(const std::shared_ptr<LBPolicyTable>& originTable,
                            const std::shared_ptr<TopoPartition>& originPartition,
                            const std::shared_ptr<TopoPartition>& newPartition)
{
    _lbPolicy.resize(8);

#define REGIST_POLICY(policy, policyType)                                                                              \
    if (!policy || !policy->init(newPartition)) {                                                                      \
        return false;                                                                                                  \
    }                                                                                                                  \
    _lbPolicy[policyType] = std::dynamic_pointer_cast<ILBPolicy>(policy);

    auto roundRobinPolicy = std::make_shared<RoundRobinPolicy>();
    REGIST_POLICY(roundRobinPolicy, LB_ROUNDROBIN);

    auto randomPolicy = std::make_shared<RandomPolicy>();
    REGIST_POLICY(randomPolicy, LB_RANDOM);

    auto weightRoundRobinPolicy = std::make_shared<WeightRoundRobinPolicy>();
    REGIST_POLICY(weightRoundRobinPolicy, LB_WEIGHT);

    auto locatingPolicy = std::make_shared<LocatingPolicy>();
    REGIST_POLICY(locatingPolicy, LB_LOCATING);

    auto conhashPolicy = std::make_shared<ConhashPolicy>();
    if (!conhashPolicy) {
        return false;
    }
    if (!conhashPolicy->inherit(originTable->getPolicy(LB_CONHASH), originPartition, newPartition)) {
        if (!conhashPolicy->init(newPartition)) {
            return false;
        }
    }
    _lbPolicy[LB_CONHASH] = std::dynamic_pointer_cast<ILBPolicy>(conhashPolicy);

    auto localPrioPolicy = std::make_shared<LocalPrioPolicy>();
    REGIST_POLICY(localPrioPolicy, LB_LOCAL_PRIO);

    auto localIDCPrioRoundRobinPolicy = std::make_shared<LocalIDCPrioRoundRobinPolicy>();
    REGIST_POLICY(localIDCPrioRoundRobinPolicy, LB_LOCAL_IDC_PRIO_ROUNDROBIN);

    auto localIDCPrioHashPolicy = std::make_shared<LocalIDCPrioHashPolicy>();
    REGIST_POLICY(localIDCPrioHashPolicy, LB_LOCAL_IDC_PRIO_HASH);

#undef REGIST_POLICY
    return true;
} // namespace cm_sub

std::shared_ptr<ILBPolicy> LBPolicyTable::getPolicy(LBPolicyType type)
{
    if (type >= _lbPolicy.size()) {
        return std::shared_ptr<ILBPolicy>();
    }
    return _lbPolicy[type];
}

std::shared_ptr<LBPolicyTable> LBPolicyTable::create(const std::shared_ptr<TopoPartition>& partition)
{
    auto table = std::make_shared<LBPolicyTable>();
    if (table && table->init(partition)) {
        return table;
    }
    return std::shared_ptr<LBPolicyTable>();
}

std::shared_ptr<LBPolicyTable> LBPolicyTable::create(const std::shared_ptr<LBPolicyTable>& originTable,
                                                     const std::shared_ptr<TopoPartition>& originPartition,
                                                     const std::shared_ptr<TopoPartition>& newPartition)
{
    if (originTable == nullptr) {
        return LBPolicyTable::create(newPartition);
    }

    auto table = std::make_shared<LBPolicyTable>();
    if (table && table->inherit(originTable, originPartition, newPartition)) {
        return table;
    }
    return nullptr;
}

} // namespace cm_sub