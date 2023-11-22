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
#include "build_service/common/SwiftResourceKeeper.h"

#include <cstdint>
#include <map>
#include <memory>
#include <ostream>
#include <stddef.h>
#include <vector>

#include "alog/Logger.h"
#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/common/SwiftAdminFacade.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/DataLinkModeUtil.h"
#include "build_service/config/ProcessorConfigReader.h"
#include "build_service/config/ProcessorConfigurator.h"
#include "build_service/util/SwiftClientCreator.h"
#include "swift/client/SwiftReaderConfig.h"
#include "swift/protocol/SwiftMessage.pb.h"

using namespace std;
using autil::StringUtil;

namespace build_service { namespace common {
BS_LOG_SETUP(common, SwiftResourceKeeper);

SwiftResourceKeeper::SwiftResourceKeeper(const std::string& name, const std::string& type,
                                         const ResourceContainerPtr& resourceContainer)
    : ResourceKeeper(name, type)
{
    if (resourceContainer) {
        resourceContainer->getResource(_topicAccessor);
    }
}

SwiftResourceKeeper::~SwiftResourceKeeper() {}

bool SwiftResourceKeeper::init(const KeyValueMap& params)
{
    _clusterName = getValueFromKeyValueMap(params, "clusterName");
    _topicConfigName = getValueFromKeyValueMap(params, "topicConfigName");
    _topicName = getValueFromKeyValueMap(params, config::SWIFT_TOPIC_NAME);
    _swiftRoot = getValueFromKeyValueMap(params, config::SWIFT_ZOOKEEPER_ROOT);
    _tag = getValueFromKeyValueMap(params, "tag");
    _resourceName = getValueFromKeyValueMap(params, "name");
    if (_clusterName.empty() || _resourceName.empty()) {
        BS_LOG(ERROR, "SwiftResourceKeeper init failed, _clusterName[%s] or _resourceName[%s] is empty().",
               _clusterName.c_str(), _resourceName.c_str());
        return false;
    }
    return true;
}

bool SwiftResourceKeeper::initLegacy(const string& clusterName, const proto::PartitionId& pid,
                                     const config::ResourceReaderPtr& configReader, const KeyValueMap& kvMap)
{
    _clusterName = getTopicClusterName(clusterName, kvMap);
    _topicConfigName = pid.step() == proto::BuildStep::BUILD_STEP_FULL ? config::FULL_SWIFT_BROKER_TOPIC_CONFIG
                                                                       : config::INC_SWIFT_BROKER_TOPIC_CONFIG;
    pair<string, string> topicInfo = getLegacyTopicName(_clusterName, pid, configReader, _topicConfigName, kvMap);
    _topicName = topicInfo.first;
    _swiftRoot = topicInfo.second;
    if (_topicName.empty() || _swiftRoot.empty()) {
        BS_LOG(ERROR, "init legacy failed, topicName [%s], swiftRoot [%s]", _topicName.c_str(), _swiftRoot.c_str());
        return false;
    }
    return true;
}

string SwiftResourceKeeper::getTopicClusterName(const string& clusterName, const KeyValueMap& kvMap)
{
    auto iter = kvMap.find(config::SHARED_TOPIC_CLUSTER_NAME);
    if (iter != kvMap.end()) {
        return iter->second;
    }
    return clusterName;
}

void SwiftResourceKeeper::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    ResourceKeeper::Jsonize(json);
    json.Jsonize("topic_config_name", _topicConfigName);
    json.Jsonize("cluster_name", _clusterName);
    json.Jsonize("tag", _tag);
    json.Jsonize("topic_name", _topicName);
    json.Jsonize("topic_id", _topicId);
    json.Jsonize("swift_root", _swiftRoot);
}

void SwiftResourceKeeper::deleteApplyer(const std::string& applyer)
{
    if (_topicName.empty()) {
        return;
    }
    if (_topicAccessor) {
        _topicAccessor->deregistBrokerTopic(applyer, _topicName);
    }
}

void SwiftResourceKeeper::syncResources(const std::string& roleName, const proto::WorkerNodes& workerNodes)
{
    string resourceStr;
    for (auto node : workerNodes) {
        proto::WorkerNodeBase::ResourceInfo* resourceInfo = NULL;
        if (!node->getTargetResource(_resourceName, resourceInfo)) {
            if (resourceStr.empty()) {
                resourceStr = ToJsonString(*this);
            }
            node->addTargetDependResource(_resourceName, resourceStr, getResourceId());
        }
    }
}

bool SwiftResourceKeeper::prepareResource(const std::string& applyer, const config::ResourceReaderPtr& configReader)
{
    if (!_topicAccessor) {
        return false;
    }
    pair<string, string> tempTopicInfo =
        _topicAccessor->registBrokerTopic(applyer, configReader, _clusterName, _topicConfigName, _tag);
    if (tempTopicInfo.first.empty() || tempTopicInfo.second.empty()) {
        return false;
    }
    if (!_topicName.empty()) {
        if (_topicName != tempTopicInfo.first || _swiftRoot != tempTopicInfo.second) {
            _topicAccessor->deregistBrokerTopic(applyer, tempTopicInfo.first);
            BS_LOG(ERROR, "SwiftResourceKeeper not support apply multi topic[%s] && topic[%s].", _topicName.c_str(),
                   tempTopicInfo.first.c_str());
            return false;
        }
    }
    _topicName = tempTopicInfo.first;
    _topicId = _topicAccessor->getTopicId(configReader, _clusterName, _topicConfigName, _tag);
    _swiftRoot = tempTopicInfo.second;
    return true;
}

pair<string, string> SwiftResourceKeeper::getLegacyTopicName(const string& clusterName, const proto::PartitionId& pid,
                                                             const config::ResourceReaderPtr& configReader,
                                                             const string& configName, const KeyValueMap& kvMap)
{
    string swiftRoot = getValueFromKeyValueMap(kvMap, config::PROCESSED_DOC_SWIFT_ROOT);
    string topicName = getValueFromKeyValueMap(kvMap, config::PROCESSED_DOC_SWIFT_TOPIC_NAME);
    if (topicName.empty()) {
        SwiftAdminFacade facade;
        if (!facade.init(pid.buildid(), configReader, _clusterName)) {
            return make_pair(string(), string());
        }
        topicName = facade.getTopicName(configName);
    }
    return make_pair(topicName, swiftRoot);
}

bool SwiftResourceKeeper::getSwiftConfig(const config::ResourceReaderPtr& configReader,
                                         config::SwiftConfig& swiftConfig)
{
    if (!configReader->getClusterConfigWithJsonPath(_clusterName, "swift_config", swiftConfig)) {
        string errorMsg = "Invalid swift config in [" + _clusterName + "].";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}

swift::client::SwiftClient*
SwiftResourceKeeper::createSwiftClient(const util::SwiftClientCreatorPtr& swiftClientCreator,
                                       const config::ResourceReaderPtr& configReader)
{
    config::SwiftConfig swiftConfig;
    if (!getSwiftConfig(configReader, swiftConfig)) {
        BS_LOG(ERROR, "create swift client failed");
        return nullptr;
    }
    string clientConfig = swiftConfig.getSwiftClientConfig(_topicConfigName);
    swift::client::SwiftClient* client = swiftClientCreator->createSwiftClient(_swiftRoot, clientConfig);
    return client;
}

common::SwiftParam SwiftResourceKeeper::createSwiftReader(const util::SwiftClientCreatorPtr& swiftClientCreator,
                                                          const KeyValueMap& params,
                                                          const config::ResourceReaderPtr& configReader,
                                                          const proto::Range& range)
{
    if (config::DataLinkModeUtil::isDataLinkNPCMode(params)) {
        return createSwiftReaderForNPCMode(swiftClientCreator, params, configReader, range);
    } else {
        return createSwiftReaderForNormalMode(swiftClientCreator, params, configReader, range);
    }
}

common::SwiftParam SwiftResourceKeeper::createSwiftReaderForNormalMode(
    const util::SwiftClientCreatorPtr& swiftClientCreator, const KeyValueMap& params,
    const config::ResourceReaderPtr& configReader, const proto::Range& range)
{
    swift::client::SwiftClient* client = createSwiftClient(swiftClientCreator, configReader);
    BS_LOG(DEBUG, "processedDocTopicName: %s", _topicName.c_str());
    if (!client) {
        BS_LOG(ERROR, "create swift reader failed as swift client create failed");
        return {};
    }
    config::SwiftConfig swiftConfig;
    if (!getSwiftConfig(configReader, swiftConfig)) {
        BS_LOG(ERROR, "create swift reader failed as invalid config");
        return {};
    }
    std::vector<std::pair<uint8_t, uint8_t>> maskPairs;
    bool enableFastSlowQueue = false;
    if (params.find(config::ENABLE_FAST_SLOW_QUEUE) != params.end()) {
        enableFastSlowQueue = true;
    }

    bool useBSInnerMaskFilter = false;
    auto iter = params.find(config::USE_INNER_BATCH_MASK_FILTER);
    if (iter != params.end() && iter->second == "true") {
        useBSInnerMaskFilter = true;
    }
    string swiftReaderConfig;
    const string& readerConfig = swiftConfig.getSwiftReaderConfig(_topicConfigName);
    stringstream ss;

    // topicname=topic1;from=100;to=200;mask=30;result=50;topicname=topic2;from=100;to=200;mask=50;result=60;
    ss << "topicName=" << _topicName << ";";
    if (!readerConfig.empty()) {
        ss << readerConfig << ";";
    }

    ss << "from=" << range.from() << ";"
       << "to=" << range.to() << ";";
    if (useBSInnerMaskFilter) {
        ss << "uint8FilterMask=0;uint8MaskResult=0;";
    } else {
        ss << "uint8FilterMask=" << getValueFromKeyValueMap(params, config::SWIFT_FILTER_MASK).c_str() << ";"
           << "uint8MaskResult=" << getValueFromKeyValueMap(params, config::SWIFT_FILTER_RESULT).c_str() << ";";
    }

    auto addMaskPair = [&](const char* maskStr, const char* resultStr) {
        uint8_t mask = 0, result = 0;
        bool convertResult = autil::StringUtil::strToUInt8(maskStr, mask);

        if (!convertResult) {
            BS_LOG(ERROR, "convert mask [%s] failed", maskStr);
            return false;
        }
        convertResult = autil::StringUtil::strToUInt8(resultStr, result);
        if (!convertResult) {
            BS_LOG(ERROR, "convert mask result [%s] failed", resultStr);
            return false;
        }
        maskPairs.push_back(std::make_pair(mask, result));
        return true;
    };
    if (!addMaskPair(getValueFromKeyValueMap(params, config::SWIFT_FILTER_MASK).c_str(),
                     getValueFromKeyValueMap(params, config::SWIFT_FILTER_RESULT).c_str())) {
        return {};
    }

    if (enableFastSlowQueue) {
        if (_swiftRoot.empty()) {
            BS_LOG(ERROR, "create swift reader failed, no zkPath");
            return {};
        }
        ss << "zkPath=" << _swiftRoot << ";"
           << "multiReaderOrder=sequence##"
           // multiReader for fast-slow-queue.
           << "topicName=" << _topicName << ";"
           << "zkPath=" << _swiftRoot << ";";
        if (!readerConfig.empty()) {
            ss << readerConfig << ";";
        }
        ss << "from=" << range.from() << ";"
           << "to=" << range.to() << ";"
           << "uint8FilterMask=" << getValueFromKeyValueMap(params, config::FAST_QUEUE_SWIFT_FILTER_MASK).c_str() << ";"
           << "uint8MaskResult=" << getValueFromKeyValueMap(params, config::FAST_QUEUE_SWIFT_FILTER_RESULT).c_str()
           << ";";
        if (!addMaskPair(getValueFromKeyValueMap(params, config::FAST_QUEUE_SWIFT_FILTER_MASK).c_str(),
                         getValueFromKeyValueMap(params, config::FAST_QUEUE_SWIFT_FILTER_RESULT).c_str())) {
            return {};
        }
    }

    swiftReaderConfig = ss.str();
    BS_LOG(INFO, "create swift reader with config %s", ss.str().c_str());
    auto reader = client->createReader(swiftReaderConfig, NULL);
    if (!reader) {
        string errorMsg = string("Fail to create swift reader for config: ") + swiftReaderConfig;
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return {};
    }
    return common::SwiftParam(client, reader, _topicName, enableFastSlowQueue, maskPairs, range.from(), range.to(),
                              useBSInnerMaskFilter);
}

common::SwiftParam SwiftResourceKeeper::createSwiftReaderForNPCMode(
    const util::SwiftClientCreatorPtr& swiftClientCreator, const KeyValueMap& params,
    const config::ResourceReaderPtr& configReader, const proto::Range& range)
{
    config::SwiftConfig swiftConfig;
    if (!getSwiftConfig(configReader, swiftConfig)) {
        BS_LOG(ERROR, "failed to get SwiftConfig");
        return {};
    }
    string clientConfig = swiftConfig.getSwiftClientConfig(_topicConfigName);
    if (clientConfig.empty()) {
        clientConfig = autil::EnvUtil::getEnv("raw_topic_swift_client_config", std::string(""));
    }
    BS_LOG(INFO, "SwiftClientConfig for topic[%s] : [%s]", _topicName.c_str(), clientConfig.c_str());
    swift::client::SwiftClient* client = swiftClientCreator->createSwiftClient(_swiftRoot, clientConfig);
    string swiftReaderConfigStr = getSwiftReaderConfigForNPCMode(swiftConfig, range);
    swift::client::SwiftReaderConfig swiftReaderConfig;
    if (!swiftReaderConfig.parseFromString(swiftReaderConfigStr)) {
        BS_LOG(ERROR, "failed to parse swift readerConfig[%s]", swiftReaderConfigStr.c_str());
        return {};
    }
    BS_LOG(INFO, "create swift reader with config %s", swiftReaderConfigStr.c_str());

    const auto& filter = swiftReaderConfig.swiftFilter;
    std::vector<std::pair<uint8_t, uint8_t>> maskPairs;
    maskPairs.push_back({filter.uint8filtermask(), filter.uint8maskresult()});
    auto reader = client->createReader(swiftReaderConfigStr, NULL);
    if (!reader) {
        string errorMsg = string("Fail to create swift reader for config: ") + swiftReaderConfigStr;
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return {};
    }
    return common::SwiftParam(client, reader, _topicName, /*enableFastSlowQueue*/ false, maskPairs, range.from(),
                              range.to(),
                              /*useBSInnerMaskFilter*/ false);
}

std::string SwiftResourceKeeper::getSwiftReaderConfigForNPCMode(const config::SwiftConfig& swiftConfig,
                                                                const proto::Range& range) const
{
    const string& topicReaderConfig = swiftConfig.getSwiftReaderConfig(_topicConfigName);
    string globalReaderConfig = autil::EnvUtil::getEnv("raw_topic_swift_reader_config", std::string(""));
    stringstream ss;
    ss << "topicName=" << _topicName << ";";
    if (!topicReaderConfig.empty()) {
        ss << topicReaderConfig << ";";
    }
    if (!globalReaderConfig.empty()) {
        ss << globalReaderConfig << ";";
    }
    ss << "from=" << range.from() << ";"
       << "to=" << range.to() << ";";
    return ss.str();
}

swift::client::SwiftWriter*
SwiftResourceKeeper::createSwiftWriter(const util::SwiftClientCreatorPtr& swiftClientCreator, const string& dataTable,
                                       const proto::DataDescription& dataDescription, const KeyValueMap& params,
                                       const config::ResourceReaderPtr& configReader)
{
    swift::client::SwiftClient* client = createSwiftClient(swiftClientCreator, configReader);
    BS_LOG(DEBUG, "processedDocTopicName: %s", _topicName.c_str());
    if (!client) {
        BS_LOG(ERROR, "create swift writer failed as swift client create failed");
        return nullptr;
    }
    config::SwiftConfig swiftConfig;
    if (!getSwiftConfig(configReader, swiftConfig)) {
        BS_LOG(ERROR, "create swift writer failed as invalid config");
        return nullptr;
    }

    uint64_t swiftWriterMaxBufferSize = 0;
    config::ProcessorConfigReaderPtr reader(new config::ProcessorConfigReader(
        configReader, dataTable, getValueFromKeyValueMap(params, config::PROCESSOR_CONFIG_NODE)));
    if (!config::ProcessorConfigurator::getSwiftWriterMaxBufferSize(reader, dataTable, _topicConfigName, _clusterName,
                                                                    dataDescription, swiftWriterMaxBufferSize)) {
        string errorMsg = "Fail to get swift writer maxBufferSize for cluster : " + _clusterName;
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return nullptr;
    } else {
        BS_LOG(INFO, "set swift writer maxBufferSize[%lu] for cluster [%s]", swiftWriterMaxBufferSize,
               _clusterName.c_str());
    }
    string writerConfig = swiftConfig.getSwiftWriterConfig(swiftWriterMaxBufferSize, _topicConfigName);
    BS_LOG(INFO, "processedDocTopicName: %s", _topicName.c_str());
    string writerConfigStr = "topicName=" + _topicName + ";" + writerConfig;
    auto appendKeyFunc = [&](const std::string& oldKey, const std::string& newKey) {
        if (params.find(oldKey) != params.end()) {
            string appendStr = ";" + newKey + "=" + params.at(oldKey);
            writerConfigStr += appendStr;
        }
    };
    appendKeyFunc(config::SWIFT_PROCESSOR_WRITER_NAME, "writerName");
    appendKeyFunc(config::SWIFT_MAJOR_VERSION, "majorVersion");
    appendKeyFunc(config::SWIFT_MINOR_VERSION, "minorVersion");
    return client->createWriter(writerConfigStr, NULL);
}

}} // namespace build_service::common
