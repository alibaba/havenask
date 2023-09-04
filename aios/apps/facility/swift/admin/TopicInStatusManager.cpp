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
#include "swift/admin/TopicInStatusManager.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "autil/TimeUtility.h"
#include "swift/common/Common.h"
#include "swift/protocol/Common.pb.h"
#include "swift/util/LogicTopicHelper.h"

namespace swift {
namespace admin {

AUTIL_LOG_SETUP(swift, TopicInStatusManager);

using namespace autil;
using namespace std;
using namespace swift::protocol;
using namespace swift::common;
using namespace swift::util;

TopicInStatus::TopicInStatus() {}

TopicInStatus::~TopicInStatus() {}

bool TopicInStatus::addPartInMetric(PartitionInStatus &metric, uint32_t netlimit, int64_t resourceLimit) {
    metric.resource =
        calcResource(metric.readRate5min, metric.writeRate5min, metric.readRequest5min, netlimit, resourceLimit);
    _rwMetrics.push_back(metric);
    return true;
}

uint32_t TopicInStatus::calcResource(uint64_t readRate5min,
                                     uint64_t writeRate5min,
                                     uint32_t readRequest5min,
                                     uint32_t netlimit,
                                     int64_t resourceLimit) const { // avoid netlimt is 0 outside
    uint32_t resource = (readRate5min + writeRate5min + writeRate5min + 45 * readRequest5min) / (6000 * netlimit);
    if (0 == resource) {
        return 1;
    }
    if (resourceLimit > 0 && resource > resourceLimit) {
        return (uint32_t)resourceLimit;
    }
    return resource;
}

uint32_t TopicInStatus::getLatestResourceAvg(uint32_t latestCount) const {
    uint32_t count = 0;
    uint32_t sum = 0;
    for (uint32_t index = _rwMetrics.forward(latestCount); index != _rwMetrics.end(); index = _rwMetrics.next(index)) {
        uint32_t resource = _rwMetrics.get(index).resource;
        if (0 == resource) {
            continue;
        }
        sum += resource;
        ++count;
    }
    if (0 != count) {
        uint32_t result = sum / count;
        return result > 0 ? result : 1;
    } else {
        return 1;
    }
}

TopicInStatusManager::TopicInStatusManager() {}

TopicInStatusManager::~TopicInStatusManager() {}

bool TopicInStatusManager::addPartInMetric(const google::protobuf::RepeatedPtrField<PartitionInMetric> &metric,
                                           uint32_t updateTime,
                                           uint32_t netlimit,
                                           int64_t resourceLimit) {
    PartitionInStatus pStatus;
    pStatus.updateTime = updateTime;
    vector<string> topicNames;
    vector<PartitionInStatus> pStatusVec;
    vector<pair<uint32_t, uint32_t>> rwTimes;
    topicNames.reserve(metric.size());
    pStatusVec.reserve(metric.size());
    rwTimes.reserve(metric.size());
    for (int idx = 0; idx < metric.size(); ++idx) {
        topicNames.emplace_back(metric.Get(idx).topicname());
        pStatus.partId = metric.Get(idx).partid();
        pStatus.writeRate1min = metric.Get(idx).writerate1min();
        pStatus.writeRate5min = metric.Get(idx).writerate5min();
        pStatus.readRate1min = metric.Get(idx).readrate1min();
        pStatus.readRate5min = metric.Get(idx).readrate5min();
        pStatus.writeRequest1min = metric.Get(idx).writerequest1min();
        pStatus.writeRequest5min = metric.Get(idx).writerequest5min();
        pStatus.readRequest1min = metric.Get(idx).readrequest1min();
        pStatus.readRequest5min = metric.Get(idx).readrequest5min();
        pStatusVec.emplace_back(pStatus);
        rwTimes.emplace_back(metric.Get(idx).lastwritetime(), metric.Get(idx).lastreadtime());
    }
    {
        ScopedWriteLock lock(_topicStatusLock);
        for (size_t idx = 0; idx < topicNames.size(); ++idx) {
            TopicInStatus &inStatus = _topicStatusMap[topicNames[idx]];
            inStatus.addPartInMetric(pStatusVec[idx], netlimit, resourceLimit);
        }
    }
    {
#define UPDATE_RWTIME(name)                                                                                            \
    auto &info = _topicRWTimeMap[name];                                                                                \
    if (info.first < rwTimes[idx].first) {                                                                             \
        info.first = rwTimes[idx].first;                                                                               \
    }                                                                                                                  \
    if (info.second < rwTimes[idx].second) {                                                                           \
        info.second = rwTimes[idx].second;                                                                             \
    }
        string logicName;
        ScopedWriteLock lock(_topicRWTimeLock);
        for (size_t idx = 0; idx < topicNames.size(); ++idx) {
            UPDATE_RWTIME(topicNames[idx]);
            if (LogicTopicHelper::getLogicTopicName(topicNames[idx], logicName, false)) {
                UPDATE_RWTIME(logicName);
            }
        }
    }
#undef UPDATE_RWTIME
    return true;
}

void TopicInStatusManager::updateTopicWriteReadTime(const TopicRWInfos &rwInfos) {
    uint32_t updateCount = 0;
    {
        ScopedWriteLock lock(_topicRWTimeLock);
        for (int idx = 0; idx < rwInfos.infos_size(); ++idx) {
            const TopicRWInfo &info = rwInfos.infos(idx);
            auto &rwTime = _topicRWTimeMap[info.topicname()];
            if (rwTime.first < info.lastwritetime()) {
                rwTime.first = info.lastwritetime();
            }
            if (rwTime.second < info.lastreadtime()) {
                rwTime.second = info.lastreadtime();
            }
            ++updateCount;
        }
    }
    AUTIL_LOG(INFO, "update %d topic write read infos", updateCount);
}

bool TopicInStatusManager::getTopicWriteReadTime(const string &topicName, TopicRWInfos &rwInfos) {
    if (topicName.empty()) {
        ScopedReadLock lock(_topicRWTimeLock);
        for (const auto &topic : _topicRWTimeMap) {
            TopicRWInfo *info = rwInfos.add_infos();
            info->set_topicname(topic.first);
            info->set_lastwritetime(topic.second.first);
            info->set_lastreadtime(topic.second.second);
        }
    } else {
        ScopedReadLock lock(_topicRWTimeLock);
        auto iter = _topicRWTimeMap.find(topicName);
        if (_topicRWTimeMap.end() == iter) {
            AUTIL_LOG(WARN, "%s not found in topic map", topicName.c_str());
            return false;
        }
        TopicRWInfo *info = rwInfos.add_infos();
        info->set_topicname(topicName);
        info->set_lastwritetime(iter->second.first);
        info->set_lastreadtime(iter->second.second);
    }
    return true;
}

void TopicInStatusManager::updateTopics(const TopicMap &topicMap) {
    {
        ScopedWriteLock lock(_topicStatusLock);
        for (auto iter = _topicStatusMap.begin(); iter != _topicStatusMap.end();) {
            if (topicMap.find(iter->first) == topicMap.end()) {
                iter = _topicStatusMap.erase(iter);
            } else {
                ++iter;
            }
        }
    }
    {
        ScopedWriteLock lock(_topicRWTimeLock);
        for (auto iter = _topicRWTimeMap.begin(); iter != _topicRWTimeMap.end();) {
            if (topicMap.find(iter->first) == topicMap.end()) {
                iter = _topicRWTimeMap.erase(iter);
            } else {
                ++iter;
            }
        }
    }
}

bool TopicInStatusManager::getPartitionResource(const string &topicName, uint32_t &resource) {
    auto iter = _topicStatusMap.find(topicName);
    if (iter == _topicStatusMap.end()) {
        return false;
    }
    resource = iter->second.getLatestResourceAvg();
    return true;
}

} // namespace admin
} // namespace swift
