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
#include "build_service/common/SwiftAdminFacade.h"

#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "autil/legacy/jsonizable.h"
#include "build_service/config/BuildServiceConfig.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/config/ResourceReaderManager.h"
#include "build_service/config/SwiftConfig.h"
#include "build_service/proto/ProtoUtil.h"
#include "fslib/fs/FileSystem.h"
#include "google/protobuf/io/zero_copy_stream_impl_lite.h"
#include "indexlib/config/TabletSchema.h"
#include "swift/common/PathDefine.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace google::protobuf;
using namespace google::protobuf::io;

using namespace swift::protocol;
using namespace swift::client;
using namespace swift::network;
using namespace indexlib::config;

using namespace build_service::config;
using namespace build_service::proto;
using namespace build_service::util;

namespace build_service { namespace common {

BS_LOG_SETUP(common, SwiftAdminFacade);

uint32_t SwiftAdminFacade::DEFAULT_SWIFT_TIMEOUT = 3000;

SwiftAdminFacade::SwiftAdminFacade(const SwiftClientCreatorPtr& creator) : _swiftClientCreator(creator)
{
    if (_swiftClientCreator.get() == nullptr) {
        _swiftClientCreator.reset(new SwiftClientCreator);
    }
}

SwiftAdminFacade::~SwiftAdminFacade() {}

SwiftAdminFacade::SwiftAdminFacade(const SwiftAdminFacade& other)
    : _buildId(other._buildId)
    , _serviceConfig(other._serviceConfig)
    , _clusterName(other._clusterName)
    , _swiftConfig(other._swiftConfig)
    , _swiftClientCreator(other._swiftClientCreator)
    , _adminSwiftConfigStr(other._adminSwiftConfigStr)
    , _resourceReader(other._resourceReader)
{
}

SwiftAdminFacade& SwiftAdminFacade::operator=(const SwiftAdminFacade& other)
{
    _buildId = other._buildId;
    _serviceConfig = other._serviceConfig;
    _clusterName = other._clusterName;
    _swiftConfig = other._swiftConfig;
    _swiftClientCreator = other._swiftClientCreator;
    _adminSwiftConfigStr = other._adminSwiftConfigStr;
    _resourceReader = other._resourceReader;
    return *this;
}

bool SwiftAdminFacade::init(const BuildId& buildId, const ResourceReaderPtr& resourceReader, const string& clusterName)
{
    _buildId = buildId;
    _clusterName = clusterName;
    _resourceReader = resourceReader;
    BuildServiceConfig serviceConfig;
    if (!_resourceReader->getConfigWithJsonPath("build_app.json", "", serviceConfig)) {
        string errorMsg = "parse build_app.json failed";
        _lastErrorMsg = errorMsg;
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    _serviceConfig = serviceConfig;
    _adminSwiftConfigStr = getAdminSwiftConfigStr();
    if (!_resourceReader->getClusterConfigWithJsonPath(_clusterName, "swift_config", _swiftConfig)) {
        string errorMsg = "Invalid swift config in [" + _clusterName + "].";
        _lastErrorMsg = errorMsg;
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    _schema = _resourceReader->getTabletSchema(_clusterName);
    if (!_schema) {
        string errorMsg = "fail to get schema for cluster[" + _clusterName + "]";
        _lastErrorMsg = errorMsg;
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}

std::string SwiftAdminFacade::getRealtimeTopicName(const std::string& applicationId, proto::BuildId buildId,
                                                   const std::string& clusterName)
{
    string suffix = applicationId;
    return ProtoUtil::getProcessedDocTopicName(suffix, buildId, clusterName);
}

std::string SwiftAdminFacade::getRealtimeTopicName(const std::string& applicationId, proto::BuildId buildId,
                                                   const std::string& clusterName,
                                                   const indexlib::config::IndexPartitionSchemaPtr& schema)
{
    string topicTag;
    auto schemaVersionId = schema->GetSchemaVersionId();
    if (schemaVersionId > 0 && !schema->HasModifyOperations()) {
        topicTag = StringUtil::toString(schemaVersionId);
    }
    string suffix = applicationId;
    suffix = topicTag.empty() ? suffix : suffix + "_" + topicTag;
    return ProtoUtil::getProcessedDocTopicName(suffix, buildId, clusterName);
}

std::string SwiftAdminFacade::getTopicId(const std::string& swiftTopicConfigName, std::string userDefineTag)
{
    std::string topicId;
    if (_schema->GetLegacySchema() && _schema->GetLegacySchema()->HasModifyOperations()) {
        topicId = "0";
    } else {
        topicId = StringUtil::toString(_schema->GetSchemaId());
    }
    if (!userDefineTag.empty()) {
        topicId = topicId.empty() ? userDefineTag : topicId + "_" + userDefineTag;
    }
    return topicId;
}

std::string SwiftAdminFacade::getTopicTag(const std::string& swiftTopicConfigName, std::string userDefineTag)
{
    bool topicConfigExist = _swiftConfig.hasTopicConfig(swiftTopicConfigName);
    string topicTag;
    if (topicConfigExist && swiftTopicConfigName != BS_TOPIC_INC) {
        if (swiftTopicConfigName == BS_TOPIC_FULL) {
            topicTag = "full";
        } else {
            topicTag = swiftTopicConfigName;
        }
    }

    auto schemaVersionId = _schema->GetSchemaId();
    if (schemaVersionId > 0 && swiftTopicConfigName != BS_TOPIC_FULL &&
        (!_schema->GetLegacySchema() || !_schema->GetLegacySchema()->HasModifyOperations())) {
        if (topicTag.empty()) {
            topicTag = StringUtil::toString(schemaVersionId);
        } else {
            topicTag = topicTag + "_" + StringUtil::toString(schemaVersionId);
        }
    }

    if (!userDefineTag.empty()) {
        if (topicTag.empty()) {
            topicTag = userDefineTag;
        } else {
            topicTag = topicTag + "_" + userDefineTag;
        }
    }
    return topicTag;
}

std::string SwiftAdminFacade::getTopicName(const std::string& swiftTopicConfigName, std::string userDefineTag)
{
    if (!_swiftConfig.hasTopicConfig(swiftTopicConfigName) && swiftTopicConfigName != BS_TOPIC_FULL &&
        swiftTopicConfigName != BS_TOPIC_INC) {
        return string();
    }
    string topicTag = getTopicTag(swiftTopicConfigName, userDefineTag);
    string suffix = _serviceConfig.getApplicationId();
    suffix = topicTag.empty() ? suffix : suffix + "_" + topicTag;
    return ProtoUtil::getProcessedDocTopicName(suffix, _buildId, _clusterName);
}

SwiftAdminAdapterPtr SwiftAdminFacade::createSwiftAdminAdapter(const std::string& zkPath)
{
    SwiftClient* swiftClient = _swiftClientCreator->createSwiftClient(zkPath, _adminSwiftConfigStr);
    if (swiftClient) {
        return swiftClient->getAdminAdapter();
    }
    return SwiftAdminAdapterPtr();
}

bool SwiftAdminFacade::deleteTopic(const std::string& topicConfigName, const string& topicName)
{
    string swiftRoot = getSwiftRoot(topicConfigName);
    SwiftAdminAdapterPtr adapter = createSwiftAdminAdapter(swiftRoot);
    if (!adapter) {
        string errorMsg = "create swift adapter  for [" + _buildId.datatable() + "] failed";
        _lastErrorMsg = errorMsg;
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    if (!deleteTopic(adapter, topicName)) {
        return false;
    }
    return true;
}

void SwiftAdminFacade::collectBatchTopicCreationInfos(const std::string& topicConfigName, const string& topicName,
                                                      TopicCreationInfoMap& infoMap)
{
    SwiftTopicConfigPtr clusterTopicConf = _swiftConfig.getTopicConfig(topicConfigName);
    assert(!topicName.empty());
    if (!clusterTopicConf) {
        BS_LOG(ERROR, "no such topicConfig [%s]", topicConfigName.c_str());
        return;
    }

    vector<TopicCreationInfo>& infoVec = infoMap[getSwiftRoot(topicConfigName)];
    TopicCreationInfo info;
    info.request = clusterTopicConf->get();
    info.request.set_topicname(topicName);
    info.topicName = topicName;

    BS_LOG(INFO, "collect single swift topic with request[%s]", info.request.ShortDebugString().c_str());
    infoVec.push_back(std::move(info));
}

bool SwiftAdminFacade::createTopic(const std::string& topicConfigName, const std::string& topicName,
                                   bool needVersionControl)
{
    SwiftAdminAdapterPtr adapter = createSwiftAdminAdapter(getSwiftRoot(topicConfigName));
    if (!adapter) {
        string errorMsg = "create swift adapter for [" + _buildId.datatable() + "] failed";
        _lastErrorMsg = errorMsg;
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    SwiftTopicConfigPtr clusterTopicConf = _swiftConfig.getTopicConfig(topicConfigName);
    if (!createTopic(adapter, topicName, *clusterTopicConf, needVersionControl)) {
        string errorMsg = "create broker topic[" + topicName + "] for cluster [" + _clusterName + "] failed.";
        _lastErrorMsg = errorMsg;
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}

bool SwiftAdminFacade::createTopic(const SwiftAdminAdapterPtr& adapter, const string& topicName,
                                   const config::SwiftTopicConfig& config, bool needVersionControl)

{
    TopicCreationRequest request = config.get();
    if (needVersionControl) {
        request.set_versioncontrol(true);
    }
    request.set_topicname(topicName);
    BS_LOG(INFO, "create swift topic with request[%s]", request.ShortDebugString().c_str());
    swift::protocol::ErrorCode ec = adapter->createTopic(request);
    if (swift::protocol::ERROR_NONE == ec) {
        BS_LOG(INFO, "create swift topic [%s] in zk [%s] done", topicName.c_str(), adapter->getZkPath().c_str());
        return true;
    } else if (swift::protocol::ERROR_ADMIN_TOPIC_HAS_EXISTED == ec) {
        BS_LOG(INFO, "topic [%s] in [%s] already exists, reuse it", topicName.c_str(), adapter->getZkPath().c_str());
        return true;
    } else {
        BS_LOG(ERROR, "create topic [%s] in [%s] failed, request [%s]", topicName.c_str(), adapter->getZkPath().c_str(),
               request.ShortDebugString().c_str());
        return false;
    }
}

bool SwiftAdminFacade::updateWriterVersion(const std::string& topicConfigName, const std::string& topicName,
                                           uint32_t majorVersion,
                                           const std::vector<std::pair<std::string, uint32_t>>& minorVersions)
{
    SwiftAdminAdapterPtr adapter = createSwiftAdminAdapter(getSwiftRoot(topicConfigName));
    if (!adapter) {
        string errorMsg = "create swift adapter for [" + _buildId.datatable() + "] failed";
        _lastErrorMsg = errorMsg;
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    SwiftTopicConfigPtr clusterTopicConf = _swiftConfig.getTopicConfig(topicConfigName);
    if (!updateWriterVersion(adapter, topicName, majorVersion, minorVersions)) {
        string errorMsg = "create broker topic[" + topicName + "] for cluster [" + _clusterName + "] failed.";
        _lastErrorMsg = errorMsg;
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}

bool SwiftAdminFacade::updateWriterVersion(const SwiftAdminAdapterPtr& adapter, const std::string& topicName,
                                           uint32_t majorVersion,
                                           const std::vector<std::pair<std::string, uint32_t>>& minorVersions)
{
    TopicWriterVersionInfo writerVersionInfo;
    writerVersionInfo.set_topicname(topicName);
    writerVersionInfo.set_majorversion(majorVersion);
    auto versions = writerVersionInfo.mutable_writerversions();
    for (auto [name, version] : minorVersions) {
        WriterVersion writerVersion;
        writerVersion.set_name(name);
        writerVersion.set_version(version);
        *(versions->Add()) = writerVersion;
    }
    BS_LOG(INFO, "update writer version with Info[%s]", writerVersionInfo.ShortDebugString().c_str());
    swift::protocol::ErrorCode ec = adapter->updateWriterVersion(writerVersionInfo);
    if (swift::protocol::ERROR_NONE == ec) {
        BS_LOG(INFO, "update writer version success, topic [%s]", topicName.c_str());
        return true;
    } else {
        BS_LOG(ERROR, "update writer version failed, topic [%s]", topicName.c_str());
        return false;
    }
}

bool SwiftAdminFacade::deleteTopic(const SwiftAdminAdapterPtr& adapter, const string& topicName)
{
    swift::protocol::ErrorCode ec = adapter->deleteTopic(topicName);
    if (swift::protocol::ERROR_NONE == ec) {
        BS_LOG(INFO, "delete swift topic [%s] in zk[%s] done", topicName.c_str(), adapter->getZkPath().c_str());
        return true;
    } else if (swift::protocol::ERROR_ADMIN_TOPIC_NOT_EXISTED == ec) {
        BS_LOG(INFO, "swift topic [%s] in zk [%s] already deleted", topicName.c_str(), adapter->getZkPath().c_str());
        return true;
    } else {
        BS_LOG(ERROR, "delete topic [%s] in zk [%s] failed.", topicName.c_str(), adapter->getZkPath().c_str());
        return false;
    }
}

std::string SwiftAdminFacade::getSwiftRoot(const std::string& topicConfigName)
{
    return topicConfigName == config::BS_TOPIC_FULL ? _serviceConfig.getSwiftRoot(true)
                                                    : _serviceConfig.getSwiftRoot(false);
}

string SwiftAdminFacade::getLastErrorMsg()
{
    string ret = _lastErrorMsg;
    _lastErrorMsg = "";
    return ret;
}

uint32_t SwiftAdminFacade::getSwiftTimeout()
{
    string param = autil::EnvUtil::getEnv(BS_ENV_ADMIN_SWIFT_TIMEOUT.c_str());
    uint32_t timeout = DEFAULT_SWIFT_TIMEOUT;
    if (param.empty() || !StringUtil::fromString(param, timeout)) {
        return DEFAULT_SWIFT_TIMEOUT;
    }
    return timeout;
}

string SwiftAdminFacade::getAdminSwiftConfigStr()
{
    return string(swift::client::CLIENT_CONFIG_ADMIN_TIMEOUT) + swift::client::CLIENT_CONFIG_KV_SEPERATOR +
           StringUtil::toString(getSwiftTimeout());
}

bool SwiftAdminFacade::updateTopicVersionControl(const std::string& topicConfigName, const std::string& topicName,
                                                 bool needVersionControl)
{
    SwiftAdminAdapterPtr adapter = createSwiftAdminAdapter(getSwiftRoot(topicConfigName));
    if (!adapter) {
        string errorMsg = "create swift adapter for [" + _buildId.datatable() + "] failed";
        _lastErrorMsg = errorMsg;
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    TopicCreationRequest request;
    swift::protocol::TopicCreationResponse response;
    request.set_topicname(topicName);
    request.set_versioncontrol(needVersionControl);
    if (swift::protocol::ERROR_NONE != adapter->modifyTopic(request, response)) {
        BS_LOG(ERROR, "modify topic [%s] failed, request [%s], response [%s]", topicName.c_str(),
               request.ShortDebugString().c_str(), response.ShortDebugString().c_str());
        return false;
    }
    return true;
}

}} // namespace build_service::common
