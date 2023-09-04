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
#include "swift/auth/TopicAclDataManager.h"

#include <functional>
#include <iosfwd>
#include <memory>
#include <utility>

#include "autil/TimeUtility.h"
#include "autil/result/Errors.h"
#include "autil/result/ForwardList.h"
#include "swift/protocol/Common.pb.h"

using namespace std;
using namespace autil;
using namespace autil::result;
using namespace swift::common;
using namespace swift::protocol;
using namespace std::placeholders;

namespace swift {
namespace auth {
AUTIL_LOG_SETUP(swift, TopicAclDataManager);

TopicAclDataManager::TopicAclDataManager() : _readOnly(false) {}

TopicAclDataManager::~TopicAclDataManager() {
    if (_dataSyncher) {
        _dataSyncher->stop();
        _dataSyncher.reset();
    }
}

bool TopicAclDataManager::init(const std::string &zkRoot, bool needBackSync, int64_t maxSyncIntervalUs) {
    _dataSyncher.reset(new TopicAclDataSyncher(std::bind(&TopicAclDataManager::setTopicAclDataMap, this, _1)));
    if (!_dataSyncher->init(zkRoot, needBackSync, maxSyncIntervalUs)) {
        return false;
    }
    _readOnly = needBackSync;
    return true;
}

autil::Result<bool> TopicAclDataManager::updateTopicAuthCheckStatus(const string &topicName,
                                                                    const TopicAuthCheckStatus &checkStatus) {
    ScopedWriteLock wlock(_rwLock);
    if (_readOnly) {
        return RuntimeError::make("readOnly mode, can't update topic check status");
    }
    auto snapshotMap = _topicAclDataMap;
    auto iter = snapshotMap.find(topicName);
    if (iter == snapshotMap.end()) {
        return RuntimeError::make("topic not exists[%s]", topicName.c_str());
    }
    iter->second.set_checkstatus(checkStatus);
    auto ret = _dataSyncher->serialized(snapshotMap);
    if (!ret.is_ok()) {
        return ret;
    }
    _topicAclDataMap = move(snapshotMap);
    return true;
}

autil::Result<bool> TopicAclDataManager::addTopicAccess(const string &topicName, const TopicAccessInfo &accessInfo) {
    ScopedWriteLock wlock(_rwLock);
    if (_readOnly) {
        return RuntimeError::make("readOnly mode, can't add topic access");
    }
    auto snapshotMap = _topicAclDataMap;
    auto iter = snapshotMap.find(topicName);
    if (iter == snapshotMap.end()) {
        return RuntimeError::make("topic not exists[%s]", topicName.c_str());
    }
    const string &accessId = accessInfo.accessauthinfo().accessid();
    for (int i = 0; i < iter->second.topicaccessinfos_size(); i++) {
        const auto &topicAccessInfo = iter->second.topicaccessinfos(i);
        if (topicAccessInfo.accessauthinfo().accessid() == accessId) {
            return RuntimeError::make("access id [%s] already in topic [%s]", accessId.c_str(), topicName.c_str());
        }
    }
    *iter->second.add_topicaccessinfos() = accessInfo;
    auto ret = _dataSyncher->serialized(snapshotMap);
    if (!ret.is_ok()) {
        return ret;
    }
    _topicAclDataMap = move(snapshotMap);
    return true;
}

autil::Result<bool> TopicAclDataManager::updateTopicAccess(const string &topicName, const TopicAccessInfo &accessInfo) {
    ScopedWriteLock wlock(_rwLock);
    if (_readOnly) {
        return RuntimeError::make("readOnly mode, can't update topic access");
    }
    auto snapshotMap = _topicAclDataMap;
    auto iter = snapshotMap.find(topicName);
    if (iter == snapshotMap.end()) {
        return RuntimeError::make("topic not exists[%s]", topicName.c_str());
    }
    const string &accessId = accessInfo.accessauthinfo().accessid();
    for (int i = 0; i < iter->second.topicaccessinfos_size(); i++) {
        TopicAccessInfo *topicAccessInfo = iter->second.mutable_topicaccessinfos(i);
        if (topicAccessInfo->accessauthinfo().accessid() == accessId) {
            *topicAccessInfo = accessInfo;
            auto ret = _dataSyncher->serialized(snapshotMap);
            if (!ret.is_ok()) {
                return ret;
            }
            _topicAclDataMap = move(snapshotMap);
            return true;
        }
    }
    return RuntimeError::make("topic accessid not exist, update failed [%s, %s]", topicName.c_str(), accessId.c_str());
}

autil::Result<bool> TopicAclDataManager::updateTopicAccessKey(const string &topicName,
                                                              const TopicAccessInfo &accessInfo) {
    ScopedWriteLock wlock(_rwLock);
    if (_readOnly) {
        return RuntimeError::make("readOnly mode, can't update topic access");
    }
    auto snapshotMap = _topicAclDataMap;
    auto iter = snapshotMap.find(topicName);
    if (iter == snapshotMap.end()) {
        return RuntimeError::make("topic not exists[%s]", topicName.c_str());
    }
    const string &accessId = accessInfo.accessauthinfo().accessid();
    for (int i = 0; i < iter->second.topicaccessinfos_size(); i++) {
        TopicAccessInfo *topicAccessInfo = iter->second.mutable_topicaccessinfos(i);
        if (topicAccessInfo->accessauthinfo().accessid() == accessId) {
            topicAccessInfo->mutable_accessauthinfo()->set_accesskey(accessInfo.accessauthinfo().accesskey());
            auto ret = _dataSyncher->serialized(snapshotMap);
            if (!ret.is_ok()) {
                return ret;
            }
            _topicAclDataMap = move(snapshotMap);
            return true;
        }
    }
    return RuntimeError::make("topic accessid not exist, update failed [%s, %s]", topicName.c_str(), accessId.c_str());
}

autil::Result<bool> TopicAclDataManager::updateTopicAccessPriority(const string &topicName,
                                                                   const TopicAccessInfo &accessInfo) {
    ScopedWriteLock wlock(_rwLock);
    if (_readOnly) {
        return RuntimeError::make("readOnly mode, can't update topic access");
    }
    auto snapshotMap = _topicAclDataMap;
    auto iter = snapshotMap.find(topicName);
    if (iter == snapshotMap.end()) {
        return RuntimeError::make("topic not exists[%s]", topicName.c_str());
    }
    const string &accessId = accessInfo.accessauthinfo().accessid();
    for (int i = 0; i < iter->second.topicaccessinfos_size(); i++) {
        TopicAccessInfo *topicAccessInfo = iter->second.mutable_topicaccessinfos(i);
        if (topicAccessInfo->accessauthinfo().accessid() == accessId) {
            topicAccessInfo->set_accesspriority(accessInfo.accesspriority());
            auto ret = _dataSyncher->serialized(snapshotMap);
            if (!ret.is_ok()) {
                return ret;
            }
            _topicAclDataMap = move(snapshotMap);
            return true;
        }
    }
    return RuntimeError::make("topic accessid not exist, update failed [%s, %s]", topicName.c_str(), accessId.c_str());
}

autil::Result<bool> TopicAclDataManager::updateTopicAccessType(const string &topicName,
                                                               const TopicAccessInfo &accessInfo) {
    ScopedWriteLock wlock(_rwLock);
    if (_readOnly) {
        return RuntimeError::make("readOnly mode, can't update topic access");
    }
    auto snapshotMap = _topicAclDataMap;
    auto iter = snapshotMap.find(topicName);
    if (iter == snapshotMap.end()) {
        return RuntimeError::make("topic not exists[%s]", topicName.c_str());
    }
    const string &accessId = accessInfo.accessauthinfo().accessid();
    for (int i = 0; i < iter->second.topicaccessinfos_size(); i++) {
        TopicAccessInfo *topicAccessInfo = iter->second.mutable_topicaccessinfos(i);
        if (topicAccessInfo->accessauthinfo().accessid() == accessId) {
            topicAccessInfo->set_accesstype(accessInfo.accesstype());
            auto ret = _dataSyncher->serialized(snapshotMap);
            if (!ret.is_ok()) {
                return ret;
            }
            _topicAclDataMap = move(snapshotMap);
            return true;
        }
    }
    return RuntimeError::make("topic accessid not exist, update failed [%s, %s]", topicName.c_str(), accessId.c_str());
}

autil::Result<bool> TopicAclDataManager::deleteTopicAccess(const string &topicName, const TopicAccessInfo &accessInfo) {
    ScopedWriteLock wlock(_rwLock);
    if (_readOnly) {
        return RuntimeError::make("readOnly mode, can't delete topic access");
    }
    auto snapshotMap = _topicAclDataMap;
    auto iter = snapshotMap.find(topicName);
    if (iter == snapshotMap.end()) {
        return RuntimeError::make("topic not exists[%s]", topicName.c_str());
    }
    TopicAclData newAccessData;
    newAccessData.set_topicname(iter->second.topicname());
    newAccessData.set_checkstatus(iter->second.checkstatus());
    const string &accessId = accessInfo.accessauthinfo().accessid();
    for (int i = 0; i < iter->second.topicaccessinfos_size(); i++) {
        const auto &topicAccessInfo = iter->second.topicaccessinfos(i);
        if (topicAccessInfo.accessauthinfo().accessid() != accessId) {
            *newAccessData.add_topicaccessinfos() = topicAccessInfo;
        }
    }
    iter->second = newAccessData;
    auto ret = _dataSyncher->serialized(snapshotMap);
    if (!ret.is_ok()) {
        return ret;
    }
    _topicAclDataMap = move(snapshotMap);
    return true;
}

autil::Result<bool> TopicAclDataManager::clearTopicAccess(const string &topicName) {
    ScopedWriteLock wlock(_rwLock);
    if (_readOnly) {
        return RuntimeError::make("readOnly mode, can't clear topic access");
    }
    auto snapshotMap = _topicAclDataMap;
    auto iter = snapshotMap.find(topicName);
    if (iter == snapshotMap.end()) {
        return RuntimeError::make("topic not exists[%s]", topicName.c_str());
    }
    iter->second.mutable_topicaccessinfos()->Clear();
    auto ret = _dataSyncher->serialized(snapshotMap);
    if (!ret.is_ok()) {
        return ret;
    }
    _topicAclDataMap = move(snapshotMap);
    return true;
}

autil::Result<bool>
TopicAclDataManager::getTopicAclData(const string &topicName, const string &accessId, TopicAclData &topicAclData) {
    ScopedReadLock rlock(_rwLock);
    auto iter = _topicAclDataMap.find(topicName);
    if (iter == _topicAclDataMap.end()) {
        return RuntimeError::make("topic not exists[%s]", topicName.c_str());
    }
    topicAclData.set_topicname(topicName);
    topicAclData.set_checkstatus(iter->second.checkstatus());
    for (int i = 0; i < iter->second.topicaccessinfos_size(); i++) {
        if (iter->second.mutable_topicaccessinfos(i)->accessauthinfo().accessid() == accessId) {
            *(topicAclData.add_topicaccessinfos()) = iter->second.topicaccessinfos(i);
            break;
        }
    }
    return true;
}

autil::Result<bool> TopicAclDataManager::getTopicAclDatas(const string &topicName, TopicAclData &accessData) {
    ScopedReadLock rlock(_rwLock);
    auto iter = _topicAclDataMap.find(topicName);
    if (iter == _topicAclDataMap.end()) {
        return RuntimeError::make("topic not exists [%s]", topicName.c_str());
    }
    accessData = iter->second;
    return true;
}

map<string, TopicAclData> TopicAclDataManager::getTopicAclDataMap() {
    ScopedReadLock rlock(_rwLock);
    return _topicAclDataMap;
}

autil::Result<bool> TopicAclDataManager::createTopicAclItems(const vector<string> &topicNames) {
    ScopedWriteLock wlock(_rwLock);
    if (_readOnly) {
        return RuntimeError::make("readOnly mode, can't create topic access");
    }
    auto snapshotMap = _topicAclDataMap;
    for (auto &topicName : topicNames) {
        auto iter = snapshotMap.find(topicName);
        if (iter != snapshotMap.end()) {
            AUTIL_LOG(INFO, "ignore existed topic[%s]", topicName.c_str());
            continue;
        }
        protocol::TopicAclData topicAclData;
        topicAclData.set_topicname(topicName);
        topicAclData.set_checkstatus(protocol::TOPIC_AUTH_CHECK_OFF);
        snapshotMap.insert(make_pair(topicName, topicAclData));
        AUTIL_LOG(INFO, "add topic [%s]  into access auth map.", topicName.c_str());
    }
    auto ret = _dataSyncher->serialized(snapshotMap);
    if (!ret.is_ok()) {
        return ret;
    }
    _topicAclDataMap = move(snapshotMap);
    return true;
}

autil::Result<bool> TopicAclDataManager::deleteTopicAclItems(const vector<string> &topicNames) {
    ScopedWriteLock wlock(_rwLock);
    if (_readOnly) {
        return RuntimeError::make("readOnly mode, can't delete topic acl items");
    }
    auto snapshotMap = _topicAclDataMap;
    for (const auto &topicName : topicNames) {
        auto iter = snapshotMap.find(topicName);
        if (iter == snapshotMap.end()) {
            AUTIL_LOG(INFO, "topic [%s] not exist in access auth map.", topicName.c_str());
            continue;
        }
        snapshotMap.erase(iter);
        AUTIL_LOG(INFO, "delete topic [%s] from access auth map.", topicName.c_str());
    }
    auto ret = _dataSyncher->serialized(snapshotMap);
    if (!ret.is_ok()) {
        return ret;
    }
    _topicAclDataMap = move(snapshotMap);
    return true;
}

void TopicAclDataManager::setTopicAclDataMap(const map<string, TopicAclData> &topicAclDataMap) {
    ScopedWriteLock wlock(_rwLock);
    _topicAclDataMap = topicAclDataMap;
}

} // namespace auth
} // namespace swift
