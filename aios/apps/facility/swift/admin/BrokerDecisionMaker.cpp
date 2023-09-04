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
#include "swift/admin/BrokerDecisionMaker.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <cstdint>
#include <list>
#include <map>
#include <memory>
#include <string>
#include <utility>

#include "autil/TimeUtility.h"
#include "swift/admin/RoleInfo.h"
#include "swift/common/PathDefine.h"
#include "swift/protocol/Common.pb.h"

namespace swift {
namespace admin {
AUTIL_LOG_SETUP(swift, BrokerDecisionMaker);
using namespace swift::protocol;
using namespace swift::common;

using namespace std;
using namespace autil;

BrokerDecisionMaker::BrokerDecisionMaker() : _scdType(ST_FLATTEN) {}
BrokerDecisionMaker::BrokerDecisionMaker(const std::string &scdType,
                                         const std::map<std::string, uint32_t> &veticalGroupBrokerCnt)
    : _veticalGroupBrokerCnt(veticalGroupBrokerCnt) {
    if ("vetical" == scdType) {
        _scdType = ST_VETICAL;
    } else {
        _scdType = ST_FLATTEN;
    }
}

BrokerDecisionMaker::~BrokerDecisionMaker() {}

void BrokerDecisionMaker::makeDecision(TopicMap &topicMap, WorkerMap &aliveBroker) {
    int64_t beginTime = TimeUtility::currentTime();
    PartitionPairList partitionPairList;
    getPartitionPairList(topicMap, partitionPairList);
    if (ST_VETICAL == _scdType) {
        vetical(partitionPairList, aliveBroker);
    } else { // ST_FLATTEN
        flatten(partitionPairList, topicMap, aliveBroker);
    }
    auto useTime = TimeUtility::currentTime() - beginTime;
    if (useTime > 1000) { // 1ms
        AUTIL_LOG(INFO, "end make decision, use time[%ldus]", useTime);
    }
}

void BrokerDecisionMaker::flatten(PartitionPairList &partitionPairList,
                                  TopicMap &topicMap,
                                  WorkerMap &aliveBroker) const {
    // first step: if admin thought partition p is loaded in broker b, and
    // broker b claims that he indeed loaded p, we should maintein this relation
    AUTIL_LOG(DEBUG, "step 1, reuse assigned false");
    reuseFormerlyAssignedBroker(partitionPairList, aliveBroker, false);

    // second step: if admin thought partition p is loaded in broker b, but
    // broker b claims that he has not loaded p, we will try to assign this
    // partition to b
    AUTIL_LOG(DEBUG, "step 2, reuse asssigned true");
    reuseFormerlyAssignedBroker(partitionPairList, aliveBroker, true);

    // third step: if partition p not assigned to any broker, but some broker
    // claimd that he has loaded p, we will let this borker reuse p only if:
    //   1. broker can load this partition
    //   2. this partition has not been dispatched to some other broker
    AUTIL_LOG(DEBUG, "step 3, reuse current");
    reuseCurrentExistPartition(topicMap, aliveBroker);

    // fourth step: assign left partition to brokers
    AUTIL_LOG(DEBUG, "step 4, assign");
    assignPartitionToBroker(partitionPairList, aliveBroker);
}

bool BrokerDecisionMaker::assign2Worker(TopicInfoPtr &tinfo, uint32_t partId, WorkerInfoPtr &targetWorker) const {
    const string &topicName = tinfo->getTopicName();
    WorkerInfo::StatusInfo statusInfo;
    if (targetWorker->addTask2target(
            topicName.c_str(), partId, tinfo->getResource(), tinfo->getPartitionLimit(), tinfo->getTopicGroup())) {
        if (targetWorker->hasTaskInCurrent(topicName, partId, statusInfo)) {
            tinfo->setSessionId(partId, statusInfo.sessionId);
            tinfo->setInlineVersion(partId, statusInfo.inlineVersion);
        } else {
            AUTIL_LOG(INFO,
                      "[%s %u] assigned to broker[%s]",
                      tinfo->getTopicName().c_str(),
                      partId,
                      targetWorker->getTargetRoleAddress().c_str());
        }
        tinfo->setTargetRoleAddress(partId, targetWorker->getTargetRoleAddress());
        tinfo->setStatus(partId, statusInfo.status);
        return true;
    }
    return false;
}

void BrokerDecisionMaker::vetical(PartitionPairList &partitionPairList, WorkerMap &aliveBroker) const {
    // 1. get group broker count
    string group;
    uint32_t brokerIndex;
    map<pair<string, uint32_t>, WorkerInfoPtr> brokers; // map<<group, broker_id>, work_info>
    map<string, vector<uint32_t>> groupBrokerIds;       // map<group, vector<broker_id>>
    for (const auto &broker : aliveBroker) {
        if (PathDefine::parseRoleGroupAndIndex(broker.first, group, brokerIndex)) {
            brokers[make_pair(group, brokerIndex)] = broker.second;
            auto &gids = groupBrokerIds[group];
            gids.push_back(brokerIndex);
        }
    }
    for (auto &groupIds : groupBrokerIds) {
        auto &vec = groupIds.second;
        std::sort(vec.begin(), vec.end());
    }

    // 2. assign part to brokers, part_(i % needBrokerCnt) -> broker_i,
    PartitionPairList::iterator iter = partitionPairList.begin();
    for (; iter != partitionPairList.end();) {
        TopicInfoPtr &tinfo = iter->first;
        const string &topicName = tinfo->getTopicName();
        const string &tgroup = tinfo->getTopicGroup();
        uint32_t partId = iter->second;
        const auto viter = _veticalGroupBrokerCnt.find(tgroup);
        if (viter == _veticalGroupBrokerCnt.end()) {
            AUTIL_LOG(WARN,
                      "[%s %d] vetical group[%s] not exist, cannot schedule",
                      topicName.c_str(),
                      partId,
                      tgroup.c_str());
            ++iter;
            continue;
        }
        uint32_t needBrokerCnt = viter->second;
        uint32_t expectBrokerId = partId % needBrokerCnt;
        auto targetBroker = brokers.find(make_pair(tgroup, expectBrokerId));
        if (targetBroker == brokers.end()) {
            ++iter;
        } else {
            if (assign2Worker(tinfo, partId, targetBroker->second)) {
                iter = partitionPairList.erase(iter);
            } else {
                AUTIL_LOG(WARN,
                          "[%s %u] assigned to broker[%s] fail",
                          topicName.c_str(),
                          partId,
                          targetBroker->second->getTargetRoleAddress().c_str());
                ++iter;
            }
        }
    }

    // 3. assign left partition to last brokers
    iter = partitionPairList.begin();
    for (; iter != partitionPairList.end();) {
        TopicInfoPtr &tinfo = iter->first;
        const string &topicName = tinfo->getTopicName();
        const string &tgroup = tinfo->getTopicGroup();
        uint32_t partId = iter->second;
        auto gbids = groupBrokerIds.find(tgroup);
        if (gbids == groupBrokerIds.end()) {
            tinfo->setStatus(partId, PARTITION_STATUS_WAITING);
            ++iter;
            continue;
        }
        const auto &tgids = gbids->second;
        int64_t pos = tgids.size() - 1;
        while (pos >= 0) {
            auto targetBroker = brokers.find(make_pair(tgroup, tgids[pos]));
            if (targetBroker != brokers.end() && assign2Worker(tinfo, partId, targetBroker->second)) {
                iter = partitionPairList.erase(iter);
                break;
            }
            --pos;
        }
        if (pos < 0) {
            AUTIL_LOG(WARN,
                      "[%s %u] not assigned, limit %d, group %s, max broker id[%u]",
                      topicName.c_str(),
                      partId,
                      tinfo->getPartitionLimit(),
                      tgroup.c_str(),
                      groupBrokerIds[tgroup].back());
            tinfo->setStatus(partId, PARTITION_STATUS_WAITING);
            ++iter;
        }
    }
}

void BrokerDecisionMaker::reuseFormerlyAssignedBroker(PartitionPairList &partitionPairList,
                                                      WorkerMap &broker,
                                                      bool tryAssign) const {
    PartitionPairList::iterator it = partitionPairList.begin();
    for (; it != partitionPairList.end();) {
        const TopicInfoPtr &topicInfoPtr = it->first;
        uint32_t partition = it->second;
        const string &roleName = topicInfoPtr->getCurrentRole(partition);
        if (roleName.empty()) {
            ++it;
            continue;
        }
        const string &topicName = topicInfoPtr->getTopicName();
        WorkerMap::iterator brokerIt = broker.find(roleName);
        if (brokerIt == broker.end()) {
            AUTIL_LOG(DEBUG,
                      "broker[%s] which contains topic[%s]-partition[%u] is now not exist",
                      roleName.c_str(),
                      topicName.c_str(),
                      partition);
            ++it;
            continue;
        }
        const WorkerInfoPtr &brokerInfoPtr = brokerIt->second;
        WorkerInfo::StatusInfo statusInfo;
        if (brokerInfoPtr->hasTaskInCurrent(topicName, partition, statusInfo) || tryAssign) {
            if (brokerInfoPtr->addTask2target(topicName,
                                              partition,
                                              topicInfoPtr->getResource(),
                                              topicInfoPtr->getPartitionLimit(),
                                              topicInfoPtr->getTopicGroup())) {
                AUTIL_LOG(DEBUG, "[%s %u] assigned to broker[%s]", topicName.c_str(), partition, roleName.c_str());
                topicInfoPtr->setTargetRoleAddress(partition, brokerInfoPtr->getTargetRoleAddress());
                topicInfoPtr->setStatus(partition, statusInfo.status);
                topicInfoPtr->setSessionId(partition, statusInfo.sessionId);
                topicInfoPtr->setInlineVersion(partition, statusInfo.inlineVersion);
                it = partitionPairList.erase(it);
                continue;
            } else {
                AUTIL_LOG(DEBUG,
                          "can't add topic[%s]-partition[%u] to target in broker[%s]",
                          topicName.c_str(),
                          partition,
                          roleName.c_str());
            }
        }
        ++it;
    }
}

void BrokerDecisionMaker::reuseCurrentExistPartition(TopicMap &topicMap, WorkerMap &aliveBroker) const {
    WorkerMap::iterator it = aliveBroker.begin();
    for (; it != aliveBroker.end(); ++it) {
        const string &roleName = it->first;
        const WorkerInfoPtr &brokerInfoPtr = it->second;
        const map<WorkerInfo::TaskPair, WorkerInfo::StatusInfo> &currentTaskStatus =
            brokerInfoPtr->getCurrentTaskStatus();
        map<WorkerInfo::TaskPair, WorkerInfo::StatusInfo>::const_iterator taskIt;
        for (taskIt = currentTaskStatus.begin(); taskIt != currentTaskStatus.end(); ++taskIt) {
            const string &topicName = (taskIt->first).first;
            uint32_t partition = (taskIt->first).second;
            WorkerInfo::StatusInfo statusInfo = taskIt->second;
            if (topicMap.find(topicName) == topicMap.end()) {
                AUTIL_LOG(DEBUG,
                          "can't find topic[%s] in topicMap which loaded in broker[%s]",
                          topicName.c_str(),
                          roleName.c_str());
                continue;
            }
            TopicInfoPtr topicInfoPtr = topicMap[topicName];
            if (partition >= topicInfoPtr->getPartitionCount()) {
                continue;
            }
            const string &targetRole = topicInfoPtr->getTargetRole(partition);
            const string &lastRole = topicInfoPtr->getLastRole(partition);
            if (targetRole == "" && (lastRole == "" || lastRole == roleName)) {
                if (brokerInfoPtr->addTask2target(topicName,
                                                  partition,
                                                  topicInfoPtr->getResource(),
                                                  topicInfoPtr->getPartitionLimit(),
                                                  topicInfoPtr->getTopicGroup())) {
                    AUTIL_LOG(DEBUG,
                              "[%s %u] assigned to "
                              "broker[%s] since this partition exists in broker",
                              topicName.c_str(),
                              partition,
                              roleName.c_str());
                    topicInfoPtr->setTargetRoleAddress(partition, brokerInfoPtr->getTargetRoleAddress());
                    topicInfoPtr->setStatus(partition, statusInfo.status);
                    topicInfoPtr->setSessionId(partition, statusInfo.sessionId);
                    topicInfoPtr->setInlineVersion(partition, statusInfo.inlineVersion);
                }
            }
        }
    }
}

void BrokerDecisionMaker::assignPartitionToBroker(PartitionPairList &partitionPairList, WorkerMap &broker) const {
    OrderedBroker orderedBroker;
    getOrderedBroker(broker, orderedBroker);

    for (PartitionPairList::iterator it = partitionPairList.begin(); it != partitionPairList.end(); ++it) {
        TopicInfoPtr topicInfoPtr = it->first;
        uint32_t partition = it->second;
        const string &role = topicInfoPtr->getTargetRole(partition);
        if (role != "") {
            AUTIL_LOG(DEBUG,
                      "[%s %u] is assigned to role [%s] ",
                      topicInfoPtr->getTopicName().c_str(),
                      partition,
                      role.c_str());
            continue;
        }
        if (isPartitionLoadedSomewhere(topicInfoPtr, partition, broker)) {
            AUTIL_LOG(DEBUG,
                      "[%s %u] is loaded somewhere, "
                      "will not assign to broker util it unloads",
                      topicInfoPtr->getTopicName().c_str(),
                      partition);
            continue;
        }
        OrderedBroker::iterator brokerIt;
        for (brokerIt = orderedBroker.begin(); brokerIt != orderedBroker.end(); ++brokerIt) {
            WorkerInfoPtr workerInfoPtr = *brokerIt;
            if (workerInfoPtr->addTask2target(topicInfoPtr->getTopicName(),
                                              partition,
                                              topicInfoPtr->getResource(),
                                              topicInfoPtr->getPartitionLimit(),
                                              topicInfoPtr->getTopicGroup())) {
                AUTIL_LOG(DEBUG,
                          "[%s %u] is now assign to broker[%s]",
                          topicInfoPtr->getTopicName().c_str(),
                          partition,
                          workerInfoPtr->getRoleInfo().roleName.c_str());
                topicInfoPtr->setTargetRoleAddress(partition, workerInfoPtr->getTargetRoleAddress());
                orderedBroker.erase(brokerIt);
                orderedBroker.insert(workerInfoPtr);
                break;
            }
        }
        if (brokerIt == orderedBroker.end()) {
            AUTIL_LOG(WARN, "[%s %u] not assigned!", topicInfoPtr->getTopicName().c_str(), partition);
        }
        topicInfoPtr->setStatus(partition, PARTITION_STATUS_WAITING);
    }
}

void BrokerDecisionMaker::getPartitionPairList(const TopicMap &topicMap, PartitionPairList &partitionPairList) const {
    TopicPriorityQueue topicQueue;
    for (TopicMap::const_iterator it = topicMap.begin(); it != topicMap.end(); ++it) {
        topicQueue.push(it->second);
    }
    while (!topicQueue.empty()) {
        TopicInfoPtr topicInfoPtr = topicQueue.top();
        topicQueue.pop();
        assert(topicInfoPtr);
        size_t count = topicInfoPtr->getPartitionCount();
        for (size_t i = 0; i < count; ++i) {
            partitionPairList.emplace_back(make_pair(topicInfoPtr, i));
        }
    }
    AUTIL_LOG(DEBUG,
              "Total topic count [%d], total partition count [%d]",
              (int)topicMap.size(),
              (int)partitionPairList.size());
}

void BrokerDecisionMaker::getOrderedBroker(const WorkerMap &broker, OrderedBroker &orderedBroker) const {
    for (WorkerMap::const_iterator it = broker.begin(); it != broker.end(); ++it) {
        orderedBroker.insert(it->second);
    }
}

bool BrokerDecisionMaker::isPartitionLoadedSomewhere(const TopicInfoPtr &topicInfoPtr,
                                                     uint32_t partition,
                                                     const WorkerMap &broker) const {
    const string &topicName = topicInfoPtr->getTopicName();
    WorkerInfo::StatusInfo statusInfo;
    for (WorkerMap::const_iterator it = broker.begin(); it != broker.end(); ++it) {
        WorkerInfoPtr brokerInfoPtr = it->second;
        if (brokerInfoPtr->hasTaskInCurrent(topicName, partition, statusInfo)) {
            return true;
        }
    }
    return false;
}

} // namespace admin
} // namespace swift
