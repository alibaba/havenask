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
#include <set>
#include <string>
#include <vector>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "swift/common/Common.h"
#include "swift/protocol/AdminRequestResponse.pb.h"

namespace swift {
namespace admin {

class NoUseTopicInfo {
public:
    NoUseTopicInfo();
    ~NoUseTopicInfo();
    NoUseTopicInfo(const NoUseTopicInfo &) = delete;
    NoUseTopicInfo &operator=(const NoUseTopicInfo &) = delete;

public:
    void clear();
    void setLastNoUseTopics(const protocol::TopicMetas &metas);
    void setLastNoUseTopics(std::map<std::string, protocol::TopicCreationRequest> topics);
    void insertNotExistTopics(const std::string &topic);
    void clearNotExistTopics();
    void getLastNoUseTopicsMeta(protocol::LastDeletedNoUseTopicResponse *response);
    void getLastNoUseTopicsMeta(const protocol::MissTopicRequest *request, protocol::TopicBatchCreationRequest &metas);
    void removeLastNoUseTopic(const protocol::TopicBatchCreationRequest &metas);
    std::vector<std::string> getNeedRecoverTopic();

private:
    mutable autil::ReadWriteLock _lastNoUseTopicLock;
    std::map<std::string, protocol::TopicCreationRequest> _lastNoUseTopics;

    mutable autil::ReadWriteLock _notExistTopicLock;
    std::set<std::string> _notExistTopics;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(NoUseTopicInfo);

} // namespace admin
} // namespace swift
