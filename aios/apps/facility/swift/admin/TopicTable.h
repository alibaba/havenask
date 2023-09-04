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

#include <set>
#include <string>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "swift/admin/TopicInfo.h"
#include "swift/common/Common.h"

namespace swift {

namespace monitor {
struct SysControlMetricsCollector;
} // namespace monitor

namespace protocol {
class TopicCreationRequest;
} // namespace protocol
} // namespace swift

namespace swift {
namespace admin {

class TopicTable {
public:
    TopicTable();
    ~TopicTable();

private:
    TopicTable(const TopicTable &);
    TopicTable &operator=(const TopicTable &);

public:
    bool updateTopic(const protocol::TopicCreationRequest *request);
    bool addTopic(const protocol::TopicCreationRequest *request);
    bool delTopic(const std::string &topicName, bool deleteData = false);
    TopicInfoPtr findTopic(const std::string &topicName);
    void getTopicMap(TopicMap &topicMap);
    void splitTopicMapBySchedule(TopicMap &scdTopics, TopicMap &noScdTopics);
    void getNeedMergeTopicMap(TopicMap &topics);
    void clear();
    void prepareDecision();
    void clearCurrentTask(const std::string &groupName);
    void getTopicNames(std::set<std::string> &topicNames);

public:
    void collectMetrics(monitor::SysControlMetricsCollector &collector);

private:
    bool addTopic(const TopicInfoPtr &topicInfoPtr);

private:
    TopicMap _topicMap;
    autil::ReadWriteLock _lock;

private:
    friend class TopicTableTest;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(TopicTable);

} // namespace admin
} // namespace swift
