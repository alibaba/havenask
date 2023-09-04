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

#include <stdint.h>
#include <string>
#include <unordered_set>
#include <vector>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "swift/admin/AdminZkDataAccessor.h"
#include "swift/common/Common.h"
#include "swift/protocol/Common.pb.h"

namespace swift {
namespace protocol {
class TopicCreationRequest;
} // namespace protocol

namespace admin {

class CleanAtDeleteManager {
public:
    CleanAtDeleteManager();
    ~CleanAtDeleteManager();
    CleanAtDeleteManager(const CleanAtDeleteManager &) = delete;
    CleanAtDeleteManager &operator=(const CleanAtDeleteManager &) = delete;

public:
    bool
    init(const std::vector<std::string> &patterns, AdminZkDataAccessorPtr dataAccessor, uint32_t cleanDataIntervalSec);
    bool needCleanDataAtOnce(const std::string &topicName);
    void pushCleanAtDeleteTopic(const std::string &topicName, const protocol::TopicCreationRequest &meta);
    void removeCleanAtDeleteTopicData(const std::unordered_set<std::string> &loadedTopicSet);
    bool isCleaningTopic(const std::string &topic);

private:
    bool existCleanAtDeleteTopic(const std::string &topic);
    bool doPushCleanAtDeleteTopic(const std::string &topic, const std::vector<std::string> &dfsRoots);
    void doRemoveCleanAtDeleteTopicData(const std::string &topic,
                                        const std::vector<std::string> &toClean,
                                        std::vector<std::string> &cleanFailed);

private:
    mutable autil::ReadWriteLock _cleanAtDeleteTasksLock;
    protocol::CleanAtDeleteTopicTasks _cleanAtDeleteTasks;
    AdminZkDataAccessorPtr _dataAccessor;
    std::vector<std::string> _patterns;
    std::unordered_set<std::string> _cleaningTopics;
    uint32_t _cleanDataIntervalSec;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(CleanAtDeleteManager);

} // namespace admin
} // namespace swift
