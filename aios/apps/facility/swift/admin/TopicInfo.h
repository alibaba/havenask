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

#include <list>
#include <map>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "swift/common/Common.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"

namespace swift {
namespace admin {

class TopicInfo {
public:
    TopicInfo(const protocol::TopicCreationRequest &request,
              protocol::PartitionStatus status = protocol::PARTITION_STATUS_WAITING);
    ~TopicInfo();

private:
    TopicInfo(const TopicInfo &);
    TopicInfo &operator=(const TopicInfo &);

public:
    std::string getTargetRole(uint32_t index) const;
    std::string getCurrentRole(uint32_t index) const;
    std::string getLastRole(uint32_t index) const;
    std::string getTargetRoleAddress(uint32_t index) const;
    std::string getCurrentRoleAddress(uint32_t index) const;
    std::string getLastRoleAddress(uint32_t index) const;
    void setCurrentRoleAddress(uint32_t index, const std::string &roleAddress);
    void setTargetRoleAddress(uint32_t index, const std::string &roleAddress);
    void setStatus(uint32_t index, protocol::PartitionStatus status);
    protocol::PartitionStatus getStatus(uint32_t index) const;
    void setSessionId(uint32_t index, int64_t sessionId);
    int64_t getSessionId(uint32_t index) const;
    protocol::TopicStatus getTopicStatus() const;
    bool needChange(uint32_t index, bool fastRecover = false, uint64_t selfVersion = 0) const;
    void setTopicMeta(const protocol::TopicCreationRequest &topicMeta);
    const protocol::TopicCreationRequest &getTopicMeta() const;
    std::string getTopicGroup() const;
    std::string getTopicName() const;
    uint32_t getPartitionCount() const;
    uint32_t getRangeCountInPartition() const;
    bool getRangeInfo(uint32_t index, uint32_t &from, uint32_t &to);
    void getPartitionRunInfo(uint32_t &runningPartitionCount,
                             uint32_t &waitingPartitionCount,
                             uint32_t &startingPartCount) const;
    uint32_t getResource() const;
    bool setResource(uint32_t newResource, uint32_t &oldResource);
    uint32_t getPartitionLimit() const;
    bool setPartitionLimit(uint32_t newLimit, uint32_t &oldLimit);
    int64_t getModifyTime() const;
    std::string getDfsRoot() const;
    bool needSchedule() const;
    protocol::TopicType getTopicType() const;
    bool hasSubPhysicTopics() const;
    std::vector<std::string> physicTopicLst() const;
    uint32_t physicTopicLstSize() const;
    bool isSealed() const;
    const std::string getLastPhysicTopicName() const;
    void setInlineVersion(uint32_t index, protocol::InlineVersion version);
    protocol::InlineVersion getInlineVersion(uint32_t index) const;
    bool brokerChanged(uint32_t index) const;
    bool enableMergeData() const;
    bool enableWriterVersionController() const;
    bool readNotCommittedMsg() const;

private:
    void calRangeInfo();

private:
    mutable autil::ReadWriteLock _lock;
    protocol::TopicCreationRequest _topicMeta;
    std::vector<protocol::PartitionStatus> _status;
    std::vector<protocol::PartitionStatus> _lastStatus;
    std::vector<std::string> _currentBrokers;
    std::vector<std::string> _targetBrokers;
    std::vector<std::string> _lastBrokers;
    std::vector<std::pair<uint16_t, uint16_t>> _ranges;
    std::vector<protocol::InlineVersion> _inlineVersions;
    std::vector<int64_t> _sessionIds;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(TopicInfo);

typedef std::map<std::string, TopicInfoPtr> TopicMap;
typedef std::pair<TopicInfoPtr, uint32_t> PartitionPair;
typedef std::list<PartitionPair> PartitionPairList;

bool operator<(const TopicInfoPtr &lhs, const TopicInfoPtr &rhs);

struct TopicInfoPtrCmp {
    bool operator()(const TopicInfoPtr &lhs, const TopicInfoPtr &rhs) { return lhs < rhs; }
};

} // namespace admin
} // namespace swift
