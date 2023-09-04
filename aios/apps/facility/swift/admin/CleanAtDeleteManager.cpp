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
#include "swift/admin/CleanAtDeleteManager.h"

#include <iosfwd>
#include <memory>

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "swift/protocol/AdminRequestResponse.pb.h"

using namespace autil;
using namespace fslib::fs;
using namespace std;
using namespace swift::protocol;

constexpr uint32_t DEFAULT_CLEAN_DATA_INTERVAL_SEC = 3600;

namespace swift {
namespace admin {
AUTIL_LOG_SETUP(swift, CleanAtDeleteManager);

CleanAtDeleteManager::CleanAtDeleteManager() : _cleanDataIntervalSec(DEFAULT_CLEAN_DATA_INTERVAL_SEC) {}

CleanAtDeleteManager::~CleanAtDeleteManager() {}

bool CleanAtDeleteManager::init(const vector<string> &patterns,
                                AdminZkDataAccessorPtr dataAccessor,
                                uint32_t cleanDataIntervalSec) {
    _cleanDataIntervalSec = cleanDataIntervalSec;
    AUTIL_LOG(INFO, "clean data interval sec[%u]", _cleanDataIntervalSec);
    if (!dataAccessor) {
        AUTIL_LOG(INFO, "ZkDataAccessor not initialized");
        return false;
    }
    _patterns = patterns;
    _dataAccessor = dataAccessor;
    if (!_dataAccessor->loadCleanAtDeleteTopicTasks(_cleanAtDeleteTasks)) {
        return false;
    }
    for (int i = 0; i < _cleanAtDeleteTasks.tasks_size(); i++) {
        _cleaningTopics.insert(_cleanAtDeleteTasks.tasks(i).topicname());
        AUTIL_LOG(INFO,
                  "load clean at delete topic task[%s %u]",
                  _cleanAtDeleteTasks.tasks(i).topicname().c_str(),
                  _cleanAtDeleteTasks.tasks(i).deletetimestampsec());
    }
    return true;
}

bool CleanAtDeleteManager::needCleanDataAtOnce(const string &topic) {
    if (existCleanAtDeleteTopic(topic)) {
        AUTIL_LOG(INFO, "clean task already exist[%s]", topic.c_str());
        return false;
    }
    for (const auto &pattern : _patterns) {
        if (topic.find(pattern) != string::npos) {
            AUTIL_LOG(INFO, "clean at delete topic found[%s]", topic.c_str());
            return true;
        }
    }
    return false;
}

bool CleanAtDeleteManager::existCleanAtDeleteTopic(const string &topic) {
    ScopedReadLock lock(_cleanAtDeleteTasksLock);
    for (int i = 0; i < _cleanAtDeleteTasks.tasks_size(); i++) {
        if (_cleanAtDeleteTasks.tasks(i).topicname() == topic) {
            return true;
        }
    }
    return false;
}

void CleanAtDeleteManager::pushCleanAtDeleteTopic(const string &topicName, const TopicCreationRequest &meta) {
    vector<string> dfsRoots;
    if (!meta.dfsroot().empty()) {
        dfsRoots.push_back(FileSystem::joinFilePath(meta.dfsroot(), topicName));
    }
    for (int i = 0; i < meta.extenddfsroot_size(); i++) {
        const string &extendPath = meta.extenddfsroot(i);
        if (!extendPath.empty()) {
            dfsRoots.push_back(FileSystem::joinFilePath(extendPath, topicName));
        }
    }
    if (dfsRoots.size() != 0) {
        if (!doPushCleanAtDeleteTopic(topicName, dfsRoots)) {
            AUTIL_LOG(WARN, "push clean at delete topics failed, may lead to dfs leak");
        }
    }
}

bool CleanAtDeleteManager::doPushCleanAtDeleteTopic(const string &topic, const vector<string> &dfsRoots) {
    ScopedWriteLock lock(_cleanAtDeleteTasksLock);
    _cleaningTopics.insert(topic);
    CleanAtDeleteTopicTask *task = _cleanAtDeleteTasks.add_tasks();
    task->set_topicname(topic);
    *task->mutable_dfspath() = {dfsRoots.begin(), dfsRoots.end()};
    task->set_deletetimestampsec(TimeUtility::currentTimeInSeconds());
    return _dataAccessor->serializeCleanAtDeleteTasks(_cleanAtDeleteTasks);
}

void CleanAtDeleteManager::removeCleanAtDeleteTopicData(const unordered_set<string> &loadedTopicSet) {
    CleanAtDeleteTopicTasks oldTasks;
    {
        ScopedWriteLock lock(_cleanAtDeleteTasksLock);
        oldTasks = _cleanAtDeleteTasks;
        _cleanAtDeleteTasks.clear_tasks();
    }
    CleanAtDeleteTopicTasks newTasks;
    for (int i = 0; i < oldTasks.tasks_size(); i++) {
        CleanAtDeleteTopicTask *task = oldTasks.mutable_tasks(i);
        if (loadedTopicSet.count(task->topicname())) {
            AUTIL_LOG(INFO, "wait for broker unload topic, plan suspended[%s]", task->topicname().c_str());
            *(newTasks.add_tasks()) = *task;
            continue;
        }
        if (task->deletetimestampsec() + _cleanDataIntervalSec > TimeUtility::currentTimeInSeconds()) {
            AUTIL_LOG(INFO,
                      "safety interval remain, plan suspended[%s %u]",
                      task->topicname().c_str(),
                      task->deletetimestampsec());
            *(newTasks.add_tasks()) = *task;
            continue;
        }
        std::vector<std::string> cleanFailed;
        std::vector<std::string> toClean;
        for (int i = 0; i < task->dfspath_size(); i++) {
            toClean.push_back(task->dfspath(i));
        }
        doRemoveCleanAtDeleteTopicData(task->topicname(), toClean, cleanFailed);
        if (cleanFailed.size() != 0) {
            task->clear_dfspath();
            *(task->mutable_dfspath()) = {cleanFailed.begin(), cleanFailed.end()};
            *(newTasks.add_tasks()) = *task;
        }
    }
    {
        ScopedWriteLock lock(_cleanAtDeleteTasksLock);
        unordered_set<string> cleaningTopics;
        for (int i = 0; i < newTasks.tasks_size(); i++) {
            *(_cleanAtDeleteTasks.add_tasks()) = newTasks.tasks(i);
            cleaningTopics.insert(newTasks.tasks(i).topicname());
        }
        _dataAccessor->serializeCleanAtDeleteTasks(_cleanAtDeleteTasks);
        _cleaningTopics = cleaningTopics;
    }
}

void CleanAtDeleteManager::doRemoveCleanAtDeleteTopicData(const string &topic,
                                                          const vector<string> &toClean,
                                                          vector<string> &cleanFailed) {
    for (int i = 0; i < toClean.size(); i++) {
        // double check path ends with topic name
        if (!StringUtil::endsWith(toClean[i], topic)) {
            AUTIL_LOG(WARN, "bad format data path for topic[%s] [%s]", topic.c_str(), toClean[i].c_str());
            continue;
        }
        const string &topicDataPath = toClean[i];
        AUTIL_LOG(INFO, "begin clean at delete task [%s] [%s]", topic.c_str(), topicDataPath.c_str());
        fslib::ErrorCode ec = FileSystem::remove(topicDataPath);
        if (fslib::EC_OK != ec && fslib::EC_NOENT != ec) {
            AUTIL_LOG(
                WARN, "clean end fail to clean topic data[%s] [%s] ec[%d]", topic.c_str(), topicDataPath.c_str(), ec);
            cleanFailed.push_back(toClean[i]);
        } else {
            AUTIL_LOG(INFO, "clean end success to clean topic data[%s] [%s]", topic.c_str(), topicDataPath.c_str());
        }
    }
}

bool CleanAtDeleteManager::isCleaningTopic(const string &topic) {
    ScopedReadLock lock(_cleanAtDeleteTasksLock);
    return _cleaningTopics.count(topic) != 0;
}

} // namespace admin
} // namespace swift
