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
#pragma once

#include <functional>
#include <map>
#include <queue>
#include <set>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "swift/admin/DecisionMaker.h"
#include "swift/admin/TopicInfo.h"
#include "swift/admin/WorkerInfo.h"
#include "swift/common/Common.h"

namespace swift {
namespace admin {

class BrokerDecisionMaker : public DecisionMaker {
    enum SCD_TYPE {
        ST_FLATTEN = 0,
        ST_VETICAL = 1,
    };

public:
    BrokerDecisionMaker();
    ~BrokerDecisionMaker() override;
    BrokerDecisionMaker(const std::string &scdType, const std::map<std::string, uint32_t> &veticalGroupBrokerCnt);
    BrokerDecisionMaker(const BrokerDecisionMaker &) = delete;
    BrokerDecisionMaker &operator=(const BrokerDecisionMaker &) = delete;

private:
    typedef std::priority_queue<TopicInfoPtr, std::vector<TopicInfoPtr>, TopicInfoPtrCmp> TopicPriorityQueue;
    typedef std::set<WorkerInfoPtr, std::greater<WorkerInfoPtr>> OrderedBroker;

public:
    void makeDecision(TopicMap &topicMap, WorkerMap &aliveBroker) override;

protected:
    bool assign2Worker(TopicInfoPtr &tinfo, uint32_t partId, WorkerInfoPtr &targetWorker) const override;

private:
    void flatten(PartitionPairList &partitionPairList, TopicMap &topicMap, WorkerMap &aliveBroker) const;
    void vetical(PartitionPairList &partitionPairList, WorkerMap &aliveBroker) const;
    void reuseFormerlyAssignedBroker(PartitionPairList &partitionPairList, WorkerMap &broker, bool tryAssign) const;
    void reuseCurrentExistPartition(TopicMap &topicMap, WorkerMap &aliveBroker) const;
    void assignPartitionToBroker(PartitionPairList &partitionPairList, WorkerMap &broker) const;
    void getOrderedBroker(const WorkerMap &broker, OrderedBroker &orderedBroker) const;
    void getPartitionPairList(const TopicMap &topicMap, PartitionPairList &partitionPairList) const;
    bool
    isPartitionLoadedSomewhere(const TopicInfoPtr &topicInfoPtr, uint32_t partition, const WorkerMap &broker) const;

private:
    SCD_TYPE _scdType;
    std::map<std::string, uint32_t> _veticalGroupBrokerCnt;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(BrokerDecisionMaker);

} // namespace admin
} // namespace swift
