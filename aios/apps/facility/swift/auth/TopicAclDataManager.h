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

#include <map>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/result/Result.h"
#include "swift/auth/TopicAclDataSyncher.h"
#include "swift/common/Common.h"
#include "swift/protocol/AdminRequestResponse.pb.h"

namespace swift {
namespace auth {

class TopicAclDataManager {
public:
    TopicAclDataManager();
    ~TopicAclDataManager();
    TopicAclDataManager(const TopicAclDataManager &) = delete;
    TopicAclDataManager &operator=(const TopicAclDataManager &) = delete;

public:
    bool init(const std::string &sysRoot, bool needSync, int64_t maxSyncIntervalUs = 10000000);
    autil::Result<bool> updateTopicAuthCheckStatus(const std::string &topicName,
                                                   const protocol::TopicAuthCheckStatus &checkStatus);
    autil::Result<bool> addTopicAccess(const std::string &topicName, const protocol::TopicAccessInfo &accessInfo);
    autil::Result<bool> updateTopicAccess(const std::string &topicName, const protocol::TopicAccessInfo &accessInfo);
    autil::Result<bool> updateTopicAccessKey(const std::string &topicName, const protocol::TopicAccessInfo &accessInfo);
    autil::Result<bool> updateTopicAccessPriority(const std::string &topicName,
                                                  const protocol::TopicAccessInfo &accessInfo);
    autil::Result<bool> updateTopicAccessType(const std::string &topicName,
                                              const protocol::TopicAccessInfo &accessInfo);
    autil::Result<bool> deleteTopicAccess(const std::string &topicName, const protocol::TopicAccessInfo &accessInfo);
    autil::Result<bool> clearTopicAccess(const std::string &topicName);
    autil::Result<bool>
    getTopicAclData(const std::string &topicName, const std::string &accessId, protocol::TopicAclData &topicAclData);
    autil::Result<bool> getTopicAclDatas(const std::string &topicName, protocol::TopicAclData &accessData);
    std::map<std::string, protocol::TopicAclData> getTopicAclDataMap();
    autil::Result<bool> createTopicAclItems(const std::vector<std::string> &topicNames);
    autil::Result<bool> deleteTopicAclItems(const std::vector<std::string> &topicNames);

public:
    void setTopicAclDataMap(const std::map<std::string, protocol::TopicAclData> &topicAclDataMap);

private:
    bool _readOnly = false;
    std::map<std::string, protocol::TopicAclData> _topicAclDataMap;
    autil::ReadWriteLock _rwLock;
    TopicAclDataSyncherPtr _dataSyncher;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(TopicAclDataManager);

} // namespace auth
} // namespace swift
