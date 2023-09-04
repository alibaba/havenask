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
#include "swift/admin/TopicTable.h"

#include <iosfwd>
#include <map>
#include <memory>
#include <stdint.h>
#include <utility>

#include "autil/TimeUtility.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "swift/monitor/AdminMetricsReporter.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/util/ProtoUtil.h"

namespace swift {
namespace admin {
AUTIL_LOG_SETUP(swift, TopicTable);

using namespace std;
using namespace autil;
using namespace swift::protocol;
using namespace fslib::fs;

TopicTable::TopicTable() {}

TopicTable::~TopicTable() {}

bool TopicTable::addTopic(const protocol::TopicCreationRequest *request) {
    PartitionStatus status =
        (TOPIC_TYPE_LOGIC == request->topictype()) ? PARTITION_STATUS_RUNNING : PARTITION_STATUS_WAITING;
    TopicInfoPtr topicInfoPtr(new TopicInfo(*request, status));
    return addTopic(topicInfoPtr);
}

// 只改变topic meta，不会变更调度信息，partition count不要修改
bool TopicTable::updateTopic(const protocol::TopicCreationRequest *request) {
    const string &topicName = request->topicname();
    ScopedWriteLock lock(_lock);
    auto iter = _topicMap.find(topicName);
    if (iter == _topicMap.end()) {
        AUTIL_LOG(ERROR, "topic not exist[%s]", topicName.c_str());
        return false;
    }
    const auto &oldMeta = iter->second->getTopicMeta();
    AUTIL_LOG(INFO, "update topic[%s] %s", topicName.c_str(), util::ProtoUtil::plainDiffStr(&oldMeta, request).c_str());
    if (TOPIC_TYPE_LOGIC == request->topictype()) {
        iter->second.reset(new TopicInfo(*request, PARTITION_STATUS_RUNNING));
    } else {
        if (oldMeta.partitioncount() != request->partitioncount()) {
            AUTIL_LOG(WARN, "topic[%s] partition count should not change", request->topicname().c_str());
        }
        iter->second->setTopicMeta(*request);
    }
    return true;
}

bool TopicTable::addTopic(const TopicInfoPtr &topicInfoPtr) {
    const string &topicName = topicInfoPtr->getTopicName();
    ScopedWriteLock lock(_lock);
    if (_topicMap.find(topicName) != _topicMap.end()) {
        AUTIL_LOG(ERROR, "Topic already exist:topicName[%s]", topicName.c_str());
        return false;
    }
    _topicMap[topicInfoPtr->getTopicName()] = topicInfoPtr;
    AUTIL_LOG(INFO, "add topic name[%s]", topicName.c_str());
    return true;
}

bool TopicTable::delTopic(const string &topicName, bool deleteData) {
    ScopedWriteLock lock(_lock);
    TopicMap::iterator it = _topicMap.find(topicName);
    if (it == _topicMap.end()) {
        AUTIL_LOG(ERROR, "Not find topic:topicName[%s]", topicName.c_str());
        return false;
    }
    if (deleteData) {
        const string &topicDataPath = FileSystem::joinFilePath(it->second->getDfsRoot(), topicName);
        if (fslib::EC_OK != FileSystem::remove(topicDataPath)) {
            AUTIL_LOG(ERROR, "delete [%s] data [%s] failed!", topicName.c_str(), topicDataPath.c_str());
        } else {
            AUTIL_LOG(INFO, "delete [%s] data [%s] success", topicName.c_str(), topicDataPath.c_str());
        }
    }
    _topicMap.erase(it);
    AUTIL_LOG(INFO, "delete topic name[%s]", topicName.c_str());
    return true;
}

TopicInfoPtr TopicTable::findTopic(const string &topicName) {
    TopicInfoPtr topicInfoPtr;
    ScopedReadLock lock(_lock);
    TopicMap::iterator it = _topicMap.find(topicName);
    if (it != _topicMap.end()) {
        topicInfoPtr = it->second;
    }
    return topicInfoPtr;
}

void TopicTable::getTopicMap(TopicMap &topicMap) {
    ScopedReadLock lock(_lock);
    topicMap = _topicMap;
}

void TopicTable::getTopicNames(set<string> &topicNames) {
    ScopedReadLock lock(_lock);
    for (const auto &pair : _topicMap) {
        topicNames.insert(pair.first);
    }
}

void TopicTable::splitTopicMapBySchedule(TopicMap &scdTopics, TopicMap &noScdTopics) {
    scdTopics.clear();
    noScdTopics.clear();
    ScopedReadLock lock(_lock);
    for (const auto &item : _topicMap) {
        if (item.second->needSchedule()) {
            scdTopics[item.first] = item.second;
        } else {
            noScdTopics[item.first] = item.second;
        }
    }
}

void TopicTable::getNeedMergeTopicMap(TopicMap &topics) {
    topics.clear();
    ScopedReadLock lock(_lock);
    for (const auto &item : _topicMap) {
        if (item.second->needSchedule() && item.second->enableMergeData()) {
            topics[item.first] = item.second;
        }
    }
}

void TopicTable::clear() {
    ScopedWriteLock lock(_lock);
    _topicMap.clear();
}

void TopicTable::prepareDecision() {
    ScopedWriteLock lock(_lock);
    for (TopicMap::iterator it = _topicMap.begin(); it != _topicMap.end(); ++it) {
        TopicInfoPtr topicInfoPtr = it->second;
        uint32_t count = topicInfoPtr->getPartitionCount();
        for (uint32_t i = 0; i < count; ++i) {
            topicInfoPtr->setTargetRoleAddress(i, "");
        }
    }
}
void TopicTable::clearCurrentTask(const std::string &groupName) {
    ScopedWriteLock lock(_lock);
    for (TopicMap::iterator it = _topicMap.begin(); it != _topicMap.end(); ++it) {
        TopicInfoPtr topicInfoPtr = it->second;
        string currentGroupName = topicInfoPtr->getTopicMeta().topicgroup();
        if (!groupName.empty() && groupName != currentGroupName) {
            continue;
        }
        uint32_t count = topicInfoPtr->getPartitionCount();
        for (uint32_t i = 0; i < count; ++i) {
            topicInfoPtr->setCurrentRoleAddress(i, "");
        }
    }
}

void TopicTable::collectMetrics(monitor::SysControlMetricsCollector &collector) {
    uint32_t totalPartCount = 0;
    uint32_t totalRunningPartCount = 0;
    uint32_t totalWaitingPartCount = 0;
    uint32_t totalStartingPartCount = 0;
    uint32_t totalTopicCount = 0;
    uint32_t totalRunningTopicCount = 0;
    uint32_t totalWaitingTopicCount = 0;
    uint32_t totalPartialRunningTopicCount = 0;
    uint32_t totalEnableMergeTopicCount = 0;
    uint32_t totalEnableMergePartCount = 0;
    {
        ScopedWriteLock lock(_lock);
        totalTopicCount = _topicMap.size();
        for (TopicMap::iterator it = _topicMap.begin(); it != _topicMap.end(); ++it) {
            uint32_t runningPartCount = 0;
            uint32_t waitingPartCount = 0;
            uint32_t startingPartCount = 0;
            uint32_t partCount = 0;
            TopicInfoPtr topicInfoPtr = it->second;
            partCount = topicInfoPtr->getPartitionCount();
            topicInfoPtr->getPartitionRunInfo(runningPartCount, waitingPartCount, startingPartCount);
            totalPartCount += partCount;
            totalStartingPartCount += startingPartCount;
            totalRunningPartCount += runningPartCount;
            totalWaitingPartCount += waitingPartCount;
            TopicStatus topicStatus = topicInfoPtr->getTopicStatus();
            if (topicStatus == TOPIC_STATUS_RUNNING) {
                totalRunningTopicCount++;
            } else if (topicStatus == TOPIC_STATUS_PARTIAL_RUNNING) {
                totalPartialRunningTopicCount++;
            } else if (topicStatus == TOPIC_STATUS_WAITING) {
                totalWaitingTopicCount++;
            }
            if (topicInfoPtr->enableMergeData()) {
                ++totalEnableMergeTopicCount;
                totalEnableMergePartCount += partCount;
            }
        }
    }
    collector.topicCount = totalTopicCount;
    float percent = totalTopicCount == 0 ? 1 : (float)totalRunningTopicCount / totalTopicCount;
    collector.runningTopicPercent = percent * 100;
    collector.runningTopicCount = totalRunningTopicCount;
    collector.waitingTopicCount = totalWaitingTopicCount;
    collector.partialRunningTopicCount = totalPartialRunningTopicCount;
    percent = totalPartCount == 0 ? 1 : (float)totalRunningPartCount / totalPartCount;
    collector.partitionCount = totalPartCount;
    collector.runningPartitionPercent = percent * 100;
    collector.runningPartitionCount = totalRunningPartCount;
    collector.waitingPartitionCount = totalWaitingPartCount;
    collector.loadingPartitionCount = totalStartingPartCount;
    collector.enableMergeTopicCount = totalEnableMergeTopicCount;
    collector.enableMergePartitionCount = totalEnableMergePartCount;
}

} // namespace admin
} // namespace swift
