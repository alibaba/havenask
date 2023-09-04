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
#include "swift/admin/modules/NoUseTopicInfo.h"

#include <algorithm>
#include <stdint.h>
#include <utility>

#include "autil/Lock.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "swift/protocol/Common.pb.h"

using namespace autil;
using namespace swift::protocol;

namespace swift {
namespace admin {
AUTIL_LOG_SETUP(swift, NoUseTopicInfo);

NoUseTopicInfo::NoUseTopicInfo() {}

NoUseTopicInfo::~NoUseTopicInfo() {}

void NoUseTopicInfo::clear() {
    {
        ScopedWriteLock lock(_lastNoUseTopicLock);
        _lastNoUseTopics.clear();
    }
    {
        ScopedWriteLock lock(_notExistTopicLock);
        _notExistTopics.clear();
    }
}

void NoUseTopicInfo::setLastNoUseTopics(const protocol::TopicMetas &metas) {
    uint32_t topicCnt = 0;
    std::vector<std::string> topicNames;
    {
        ScopedWriteLock lock(_lastNoUseTopicLock);
        _lastNoUseTopics.clear();
        for (int idx = 0; idx < metas.topicmetas_size(); ++idx) {
            const TopicCreationRequest &meta = metas.topicmetas(idx);
            _lastNoUseTopics[meta.topicname()] = meta;
            topicNames.push_back(meta.topicname());
        }
        topicCnt = _lastNoUseTopics.size();
    }
    AUTIL_LOG(INFO, "Finish load [%d] no use topics [%s]", topicCnt, StringUtil::toString(topicNames).c_str());
}

void NoUseTopicInfo::setLastNoUseTopics(std::map<std::string, protocol::TopicCreationRequest> topics) {
    ScopedWriteLock lock(_lastNoUseTopicLock);
    _lastNoUseTopics = std::move(topics);
}

void NoUseTopicInfo::insertNotExistTopics(const std::string &topic) {
    ScopedWriteLock lock(_notExistTopicLock);
    _notExistTopics.insert(topic);
}

void NoUseTopicInfo::clearNotExistTopics() {
    ScopedWriteLock lock(_notExistTopicLock);
    _notExistTopics.clear();
}

void NoUseTopicInfo::getLastNoUseTopicsMeta(protocol::LastDeletedNoUseTopicResponse *response) {
    ScopedReadLock lock(_lastNoUseTopicLock);
    for (const auto &item : _lastNoUseTopics) {
        *response->add_topicmetas() = item.second;
    }
}

void NoUseTopicInfo::getLastNoUseTopicsMeta(const protocol::MissTopicRequest *request,
                                            protocol::TopicBatchCreationRequest &metas) {
    ScopedReadLock lock(_lastNoUseTopicLock);
    for (int idx = 0; idx < request->topicnames_size(); ++idx) {
        const std::string &topicName = request->topicnames(idx);
        const auto iter = _lastNoUseTopics.find(topicName);
        if (_lastNoUseTopics.end() != iter) {

            *metas.add_topicrequests() = iter->second;
        }
    }
}

void NoUseTopicInfo::removeLastNoUseTopic(const protocol::TopicBatchCreationRequest &metas) {
    ScopedWriteLock lock(_lastNoUseTopicLock);
    for (int idx = 0; idx < metas.topicrequests_size(); ++idx) {
        _lastNoUseTopics.erase(metas.topicrequests(idx).topicname());
    }
}

std::vector<std::string> NoUseTopicInfo::getNeedRecoverTopic() {
    std::vector<std::string> recoverTopics;
    ScopedReadLock lock(_notExistTopicLock);
    AUTIL_LOG(INFO, "last no use topics is [%s]", StringUtil::toString(_notExistTopics).c_str());
    if (_notExistTopics.empty()) {
        return recoverTopics;
    }
    ScopedReadLock lock2(_lastNoUseTopicLock);
    for (auto &topic : _notExistTopics) {
        if (_lastNoUseTopics.end() != _lastNoUseTopics.find(topic)) {
            recoverTopics.emplace_back(topic);
        }
    }
    return recoverTopics;
}

} // namespace admin
} // namespace swift
