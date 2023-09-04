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
#include "swift/auth/TopicAclDataSyncher.h"

#include <cstddef>
#include <memory>
#include <utility>

#include "autil/CommonMacros.h"
#include "autil/TimeUtility.h"
#include "autil/result/Errors.h"
#include "autil/result/ForwardList.h"
#include "fslib/util/FileUtil.h"
#include "swift/common/PathDefine.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "zookeeper/zookeeper.h"

using namespace autil;
using namespace autil::result;
using namespace std;
using namespace swift::common;
using namespace swift::protocol;
using namespace std::placeholders;

namespace swift {
namespace auth {
AUTIL_LOG_SETUP(swift, TopicAclDataSyncher);

TopicAclDataSyncher::TopicAclDataSyncher(const CallbackFuncType &callback) : _callbackFunc(callback) {}

TopicAclDataSyncher::~TopicAclDataSyncher() { stop(); }

bool TopicAclDataSyncher::init(const std::string &zkRoot, bool needBackSync, int64_t maxSyncIntervalUs) {
    if (_zkWrapper != nullptr) {
        return true;
    }
    string zkHost = fslib::util::FileUtil::getHostFromZkPath(zkRoot);
    if (zkHost.empty()) {
        AUTIL_LOG(ERROR, "invalid zkRoot:[%s]", zkRoot.c_str());
        return false;
    }
    _zkWrapper = new cm_basic::ZkWrapper(zkHost);
    if (!_zkWrapper->open()) {
        AUTIL_LOG(ERROR, "open zkRoot:[%s] failed", zkRoot.c_str());
        return false;
    }
    _zkPath = PathDefine::getPathFromZkPath(zkRoot);
    if (!doSync()) {
        return false;
    }
    if (needBackSync) {
        if (!registerSyncCallback()) {
            AUTIL_LOG(ERROR, "topic acl data synchronizer start failed.");
            return false;
        }
        _loopThread = LoopThread::createLoopThread(
            std::bind(&TopicAclDataSyncher::doSync, this), maxSyncIntervalUs, "sync_topic_access_data");
        if (!_loopThread) {
            return false;
        }
    }
    return true;
}

void TopicAclDataSyncher::stop() {
    if (_loopThread) {
        _loopThread->stop();
    }
    unregisterSyncCallback();
    if (_zkWrapper) {
        DELETE_AND_SET_NULL(_zkWrapper);
    }
}

bool TopicAclDataSyncher::doSync() {
    ScopedLock lock(_mutex);
    std::map<std::string, protocol::TopicAclData> accessDataMap;
    auto ret = deserialized(accessDataMap);
    if (!ret.is_ok()) {
        AUTIL_LOG(ERROR, "sync topic access data failed[%s]", ret.get_error().message().c_str());
        return false;
    }
    AUTIL_LOG(INFO, "sync topic access data map size[%ld]", accessDataMap.size());
    if (_callbackFunc) {
        _callbackFunc(accessDataMap);
    }
    return true;
}

bool TopicAclDataSyncher::registerSyncCallback() {
    if (_zkWrapper == nullptr) {
        return false;
    }
    _zkWrapper->setConnCallback(bind(&TopicAclDataSyncher::dataChange, this, _1, _2, _3));
    _zkWrapper->setDataCallback(bind(&TopicAclDataSyncher::dataChange, this, _1, _2, _3));
    _zkWrapper->setCreateCallback(bind(&TopicAclDataSyncher::dataChange, this, _1, _2, _3));
    return true;
}

void TopicAclDataSyncher::unregisterSyncCallback() {
    if (_zkWrapper == nullptr) {
        return;
    }
    _zkWrapper->setConnCallback(0);
    _zkWrapper->setDataCallback(0);
    _zkWrapper->setCreateCallback(0);
}

void TopicAclDataSyncher::dataChange(cm_basic::ZkWrapper *zk,
                                     const string &path,
                                     cm_basic::ZkWrapper::ZkStatus status) {
    (void)zk;
    AUTIL_LOG(INFO, "data change path [%s], status [%d]", path.c_str(), int(status));
    doSync();
}

autil::Result<bool> TopicAclDataSyncher::serialized(const map<string, TopicAclData> &accessDataMap) {
    string topicAclDataFile = PathDefine::getTopicAclDataFile(_zkPath);
    AllTopicAclData allTopicAclData;
    for (const auto &iter : accessDataMap) {
        *(allTopicAclData.add_alltopicacldata()) = iter.second;
    }
    string aclData;
    allTopicAclData.SerializeToString(&aclData);
    TopicAclDataHeader header;
    header.version = 0;
    header.compress = 0;
    header.len = aclData.size();
    header.topicCount = accessDataMap.size();
    header.checksum = TopicAclDataHeader::calcChecksum(aclData);
    string content((char *)&header, sizeof(header));
    content += aclData;
    if (!_zkWrapper->touch(topicAclDataFile, content, true)) {
        return RuntimeError::make("serialized topic access datas [%s] failed", topicAclDataFile.c_str());
    }
    return true;
}

autil::Result<bool> TopicAclDataSyncher::deserialized(map<string, TopicAclData> &accessDataMap) {
    string topicAclDataFile = PathDefine::getTopicAclDataFile(_zkPath);
    bool exist = true;
    if (!_zkWrapper->check(topicAclDataFile, exist, true)) {
        return RuntimeError::make("check topic auth file [%s] failed.", topicAclDataFile.c_str());
    }
    if (!exist) {
        AUTIL_LOG(INFO, "topic auth file [%s] not exist.", topicAclDataFile.c_str());
        return true;
    }
    string content;
    if (ZOK != _zkWrapper->getData(topicAclDataFile, content, true)) {
        return RuntimeError::make("load topic acl datas [%s] failed", topicAclDataFile.c_str());
    }
    if (content.empty()) {
        AUTIL_LOG(INFO, "deserialized topic acl data map empty");
        return true;
    }
    size_t headerLen = sizeof(TopicAclDataHeader);
    TopicAclDataHeader *header = (TopicAclDataHeader *)(content.c_str());
    if (content.size() < headerLen || content.size() != headerLen + header->len) {
        return RuntimeError::make("topic acl data is invalid [%s], len [%ld]", content.c_str(), content.size());
    }
    string aclData = content.substr(headerLen);
    if (header->checksum != TopicAclDataHeader::calcChecksum(aclData)) {
        return RuntimeError::make("topic acl data checksum is invalid expect [%u], accual [%u]",
                                  header->checksum,
                                  TopicAclDataHeader::calcChecksum(aclData));
    }
    AllTopicAclData allTopicAclData;
    if (!allTopicAclData.ParseFromString(aclData)) {
        return RuntimeError::make("parse acl data failed [%s]", content.c_str());
    }
    accessDataMap.clear();
    for (int i = 0; i < allTopicAclData.alltopicacldata_size(); i++) {
        const auto &accessData = allTopicAclData.alltopicacldata(i);
        accessDataMap[accessData.topicname()] = accessData;
    }
    if (header->topicCount != accessDataMap.size()) {
        return RuntimeError::make(
            "deserialized topic count not equal, %u vs %ld", header->topicCount, accessDataMap.size());
    }
    return true;
}

} // namespace auth
} // namespace swift
