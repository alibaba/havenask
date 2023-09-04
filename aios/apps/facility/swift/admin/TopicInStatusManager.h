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
#include <utility>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "swift/admin/TopicInfo.h"
#include "swift/util/CircularVector.h"

namespace google {
namespace protobuf {
template <typename T>
class RepeatedPtrField;
} // namespace protobuf
} // namespace google

namespace swift {
namespace protocol {
class PartitionInMetric;
class TopicRWInfos;
} // namespace protocol

namespace admin {

struct PartitionInStatus {
    uint32_t partId = 0;
    uint32_t updateTime = 0;
    uint64_t writeRate1min = 0;
    uint64_t writeRate5min = 0;
    uint64_t readRate1min = 0;
    uint64_t readRate5min = 0;
    uint32_t writeRequest1min = 0;
    uint32_t writeRequest5min = 0;
    uint32_t readRequest1min = 0;
    uint32_t readRequest5min = 0;
    uint32_t resource = 0;
};

class TopicInStatus {
public:
    TopicInStatus();
    ~TopicInStatus();

private:
    TopicInStatus(const TopicInStatus &);
    TopicInStatus &operator=(const TopicInStatus &);
    uint32_t calcResource(uint64_t readRate5min,
                          uint64_t writeRate5min,
                          uint32_t readRequest5min,
                          uint32_t netlimit,
                          int64_t resourceLimit = -1) const;

public:
    bool addPartInMetric(PartitionInStatus &metric, uint32_t netlimit = 500, int64_t resourceLimit = -1);
    uint32_t getLatestResourceAvg(uint32_t latestCount = 4) const;

private:
    util::CircularVector<PartitionInStatus> _rwMetrics;
};

class TopicInStatusManager {
public:
    TopicInStatusManager();
    ~TopicInStatusManager();

private:
    TopicInStatusManager(const TopicInStatusManager &);
    TopicInStatusManager &operator=(const TopicInStatusManager &);

private:
    void calcTopicResource(uint32_t totalResource);

public:
    bool addPartInMetric(const google::protobuf::RepeatedPtrField<protocol::PartitionInMetric> &metric,
                         uint32_t updateTime,
                         uint32_t netlimit,
                         int64_t resourceLimit);
    void updateTopicWriteReadTime(const protocol::TopicRWInfos &rwInfos);
    bool getTopicWriteReadTime(const std::string &topicName, protocol::TopicRWInfos &rwInfos);
    void updateTopics(const TopicMap &topicMap);
    bool getPartitionResource(const std::string &topicName, uint32_t &resource);

private:
    // map<TopicName, TopicInStatus>
    mutable autil::ReadWriteLock _topicStatusLock;
    std::map<std::string, TopicInStatus> _topicStatusMap;

    // map<TopicName, pair<lastWriteTime, lastReadTime> >, map default int value is 0
    mutable autil::ReadWriteLock _topicRWTimeLock;
    std::map<std::string, std::pair<uint32_t, uint32_t>> _topicRWTimeMap;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace admin
} // namespace swift
