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
#include "build_service/admin/BatchBrokerTopicAccessor.h"

#include "autil/TimeUtility.h"
#include "build_service/common/SwiftAdminFacade.h"

using namespace std;
using namespace autil;
using namespace build_service::common;

using namespace swift::protocol;
using namespace swift::client;
using namespace swift::network;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, BatchBrokerTopicAccessor);

const size_t BatchBrokerTopicAccessor::DEFAULT_MAX_BATCH_SIZE = 128;

BatchBrokerTopicAccessor::BatchBrokerTopicAccessor(proto::BuildId buildId, size_t maxBatchSize)
    : BrokerTopicAccessor(buildId)
    , _retryInterval(0)
    , _lastRetryTimestamp(0)
    , _maxBatchSize(maxBatchSize)
{
}

BatchBrokerTopicAccessor::~BatchBrokerTopicAccessor() {}

bool BatchBrokerTopicAccessor::prepareBrokerTopics()
{
    if (!canRetry()) {
        return false;
    }

    TopicCreationInfoMap infoMap;
    for (const auto& [topicName, brokerTopicKeeper] : _usingBrokerTopics) {
        auto rolesIter = _brokerTopicApplers.find(topicName);
        if (rolesIter == _brokerTopicApplers.end() || rolesIter->second.size() == 0) {
            continue;
        }
        brokerTopicKeeper->collectBatchTopicCreationInfos(infoMap);
    }
    bool ret = true;
    for (const auto& [swiftRoot, topicInfos] : infoMap) {
        if (topicInfos.empty()) {
            continue;
        }
        SwiftAdminAdapterPtr adapter = createSwiftAdminAdapter(swiftRoot);
        if (!adapter) {
            BS_LOG(ERROR, "create swift adapter for [%s] in swiftRoot [%s] failed", _buildId.datatable().c_str(),
                   swiftRoot.c_str());
            ret = false;
            continue;
        }

        if (!BatchCreateTopic(adapter, topicInfos)) {
            ret = false;
        }
    }
    updateRetryStatus(ret);
    return ret;
}

bool BatchBrokerTopicAccessor::clearUselessBrokerTopics(bool clearAll)
{
    if (!canRetry()) {
        return false;
    }

    bool ret = true;
    BatchDeleteTopicInfoMap batchDeleteInfoMap;
    makeBatchDeleteRequest(clearAll, batchDeleteInfoMap);
    for (const auto& [swiftRoot, batchDeleteTopicInfo] : batchDeleteInfoMap) {
        auto& uselessBrokerTopicKeys = batchDeleteTopicInfo.uselessBrokerTopicKeys;
        vector<string> toDeleteTopicNames(batchDeleteTopicInfo.toDeleteTopicNames.begin(),
                                          batchDeleteTopicInfo.toDeleteTopicNames.end());

        if (!BatchDeleteTopic(swiftRoot, toDeleteTopicNames)) {
            ret = false;
            continue;
        }

        for (auto& key : uselessBrokerTopicKeys) {
            _usingBrokerTopics.erase(key);
            _brokerTopicApplers.erase(key);
        }
    }
    updateRetryStatus(ret);
    return ret;
}

bool BatchBrokerTopicAccessor::canRetry()
{
    int64_t currentTime = TimeUtility::currentTimeInSeconds();
    return currentTime - _lastRetryTimestamp > _retryInterval;
}

void BatchBrokerTopicAccessor::updateRetryStatus(bool success)
{
    if (success) {
        _lastRetryTimestamp = 0;
        _retryInterval = 0;
        return;
    }
    _retryInterval = _retryInterval == 0 ? 1 : _retryInterval * 2;
    if (_retryInterval > MAX_RETRY_INTERVAL) {
        _retryInterval = MAX_RETRY_INTERVAL;
    }
    _lastRetryTimestamp = TimeUtility::currentTimeInSeconds();
}

SwiftAdminAdapterPtr BatchBrokerTopicAccessor::createSwiftAdminAdapter(const string& zkPath)
{
    SwiftClient* swiftClient =
        _swiftClientCreator->createSwiftClient(zkPath, SwiftAdminFacade::getAdminSwiftConfigStr());
    if (swiftClient) {
        return swiftClient->getAdminAdapter();
    }
    return SwiftAdminAdapterPtr();
}

void BatchBrokerTopicAccessor::markCreateTopicSuccess(const string& topicName)
{
    string brokerTopicKey = topicName;
    auto iter = _usingBrokerTopics.find(brokerTopicKey);
    if (iter == _usingBrokerTopics.end()) {
        BS_LOG(ERROR, "brokerTopicKey [%s] not found!", brokerTopicKey.c_str());
        return;
    }
    iter->second->markCreatedTopic();
}

void BatchBrokerTopicAccessor::reportCreateTopicFail(const string& topicName)
{
    string brokerTopicKey = topicName;
    auto iter = _usingBrokerTopics.find(brokerTopicKey);
    if (iter == _usingBrokerTopics.end()) {
        BS_LOG(ERROR, "brokerTopicKey [%s] not found!", brokerTopicKey.c_str());
        return;
    }
    iter->second->reportCreateTopicFail();
}

void BatchBrokerTopicAccessor::handleBatchCreateExistError(const string& errorMsg,
                                                           const vector<TopicCreationInfo>& topicInfos)
{
    BS_LOG(INFO,
           "batch create topic find some topicNames already exist, "
           "reuse them [%s]",
           errorMsg.c_str());

    // ERROR_ADMIN_TOPIC_HAS_EXISTED[topicName1;topicName2;...]
    size_t pos = errorMsg.find_first_of("[");
    if (pos == string::npos || *errorMsg.rbegin() != ']') {
        BS_LOG(ERROR,
               "wrong format for batch create topic "
               "with TopicExist error [%s]",
               errorMsg.c_str());
        return;
    }

    string existTopicStr = errorMsg.substr(pos + 1, errorMsg.length() - pos - 2);
    vector<string> topicNames = StringUtil::split(existTopicStr, ";");
    for (auto& topic : topicNames) {
        BS_LOG(INFO, "reuse exist topic [%s].", topic.c_str());
        for (size_t i = 0; i < topicInfos.size(); i++) {
            auto& singleReq = topicInfos[i];
            if (singleReq.request.topicname() == topic) {
                markCreateTopicSuccess(singleReq.topicName);
            }
        }
    }
}

void BatchBrokerTopicAccessor::makeBatchDeleteRequest(bool clearAll, BatchDeleteTopicInfoMap& batchDeleteInfoMap) const
{
    if (clearAll) {
        for (const auto& [topicName, brokerTopicKeeper] : _usingBrokerTopics) {
            auto swiftRoot = brokerTopicKeeper->getSwiftRoot();
            brokerTopicKeeper->collectDeleteAllTopic(batchDeleteInfoMap);
            batchDeleteInfoMap[swiftRoot].uselessBrokerTopicKeys.insert(topicName);
        }
        return;
    }

    for (const auto& [topicName, brokerTopicKeeper] : _usingBrokerTopics) {
        auto rolesIter = _brokerTopicApplers.find(topicName);
        if (rolesIter == _brokerTopicApplers.end() || rolesIter->second.empty()) {
            auto swiftRoot = brokerTopicKeeper->getSwiftRoot();
            brokerTopicKeeper->collectDeleteAllTopic(batchDeleteInfoMap);
            batchDeleteInfoMap[swiftRoot].uselessBrokerTopicKeys.insert(topicName);
        }
    }
}

BrokerTopicKeeperPtr BatchBrokerTopicAccessor::findBrokerTopicKeeper(const string& topicName) const
{
    auto iter = _usingBrokerTopics.find(topicName);
    if (iter != _usingBrokerTopics.end()) {
        return iter->second;
    }
    return BrokerTopicKeeperPtr();
}

void BatchBrokerTopicAccessor::reportBatchDeleteTopic(const vector<string>& toDeleteTopicNames, bool success)
{
    for (auto topicName : toDeleteTopicNames) {
        auto topicKeeper = findBrokerTopicKeeper(topicName);
        if (topicKeeper) {
            topicKeeper->reportDeleteTopic(success);
        }
    }
}

bool BatchBrokerTopicAccessor::BatchCreateTopic(const SwiftAdminAdapterPtr& adapter,
                                                const vector<TopicCreationInfo>& topicInfos)
{
    if (topicInfos.size() <= _maxBatchSize) {
        return doBatchCreateTopic(adapter, topicInfos);
    }

    vector<TopicCreationInfo> targetTopicInfos;
    targetTopicInfos.reserve(_maxBatchSize);

    for (auto& topicInfo : topicInfos) {
        targetTopicInfos.push_back(topicInfo);
        if (targetTopicInfos.size() >= _maxBatchSize) {
            if (!doBatchCreateTopic(adapter, targetTopicInfos)) {
                return false;
            }
            targetTopicInfos.clear();
        }
    }
    return targetTopicInfos.empty() ? true : doBatchCreateTopic(adapter, targetTopicInfos);
}

bool BatchBrokerTopicAccessor::doBatchCreateTopic(const SwiftAdminAdapterPtr& adapter,
                                                  const vector<TopicCreationInfo>& topicInfos)
{
    set<string> topicSet;
    TopicBatchCreationRequest request;
    for (auto& singleReq : topicInfos) {
        const string& topicName = singleReq.request.topicname();
        auto ret = topicSet.insert(topicName);
        if (!ret.second) {
            BS_LOG(WARN, "duplicate topicName [%s] in one createTopicBatch request, use first topic config!",
                   topicName.c_str());
            continue;
        }
        *(request.mutable_topicrequests()->Add()) = singleReq.request;
    }

    BS_LOG(INFO, "batch create [%lu] swift topics with createTopicBatch request", topicInfos.size());
    swift::protocol::TopicBatchCreationResponse response;
    swift::protocol::ErrorCode ec = adapter->createTopicBatch(request, response);
    if (swift::protocol::ERROR_NONE == ec) {
        for (auto& singleReq : topicInfos) {
            BS_LOG(INFO, "batch create topic [%s] done.", singleReq.request.topicname().c_str());
            markCreateTopicSuccess(singleReq.topicName);
        }
        return true;
    }

    if (swift::protocol::ERROR_ADMIN_TOPIC_HAS_EXISTED == ec) {
        handleBatchCreateExistError(response.errorinfo().errmsg(), topicInfos);
        return true;
    }

    // batch create topic fail
    for (auto& singleReq : topicInfos) {
        BS_LOG(ERROR, "batch create topic [%s] failed.", singleReq.request.topicname().c_str());
        reportCreateTopicFail(singleReq.topicName);
    }
    return false;
}

bool BatchBrokerTopicAccessor::BatchDeleteTopic(const string& swiftRoot, const vector<string>& toDeleteTopicNames)
{
    if (toDeleteTopicNames.empty()) {
        return true;
    }

    SwiftAdminAdapterPtr adapter = createSwiftAdminAdapter(swiftRoot);
    if (!adapter) {
        BS_LOG(ERROR, "create swift adapter for [%s] in swiftRoot [%s] failed", _buildId.datatable().c_str(),
               swiftRoot.c_str());
        return false;
    }

    if (toDeleteTopicNames.size() <= _maxBatchSize) {
        return doBatchDeleteTopic(adapter, toDeleteTopicNames);
    }

    vector<string> targetDeleteTopics;
    targetDeleteTopics.reserve(_maxBatchSize);

    for (auto& topic : toDeleteTopicNames) {
        targetDeleteTopics.push_back(topic);
        if (targetDeleteTopics.size() >= _maxBatchSize) {
            if (!doBatchDeleteTopic(adapter, targetDeleteTopics)) {
                return false;
            }
            targetDeleteTopics.clear();
        }
    }
    return targetDeleteTopics.empty() ? true : doBatchDeleteTopic(adapter, targetDeleteTopics);
}

bool BatchBrokerTopicAccessor::doBatchDeleteTopic(const SwiftAdminAdapterPtr& adapter,
                                                  const vector<string>& toDeleteTopicNames)
{
    swift::protocol::TopicBatchDeletionRequest request;
    for (auto topic : toDeleteTopicNames) {
        BS_LOG(INFO, "topic [%s] will be deleted in batch request", topic.c_str());
        request.add_topicnames(topic);
    }
    BS_LOG(INFO, "batch delete [%lu] swift topics with deleteTopicBatch request", toDeleteTopicNames.size());
    swift::protocol::TopicBatchDeletionResponse response;
    swift::protocol::ErrorCode ec = adapter->deleteTopicBatch(request, response);
    if (swift::protocol::ERROR_NONE == ec) {
        BS_LOG(INFO, "batch delete [%lu] swift topics done", toDeleteTopicNames.size());
        reportBatchDeleteTopic(toDeleteTopicNames, true);
        return true;
    }

    if (swift::protocol::ERROR_ADMIN_TOPIC_NOT_EXISTED == ec) {
        BS_LOG(INFO, "[%lu] swift topics already deleted", toDeleteTopicNames.size());
        reportBatchDeleteTopic(toDeleteTopicNames, true);
        return true;
    }

    BS_LOG(ERROR, "batch delete [%lu] swift topics fail, ErrorCode [%s]!", toDeleteTopicNames.size(),
           ErrorCode_Name(ec).c_str());
    reportBatchDeleteTopic(toDeleteTopicNames, false);
    return false;
}
}} // namespace build_service::admin
