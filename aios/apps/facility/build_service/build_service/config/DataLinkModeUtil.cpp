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
#include "build_service/config/DataLinkModeUtil.h"

#include <iosfwd>
#include <map>
#include <utility>

#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/SwiftTopicConfig.h"

using namespace std;

namespace build_service { namespace config {
BS_LOG_SETUP(config, DataLinkModeUtil);

DataLinkModeUtil::DataLinkModeUtil() {}

DataLinkModeUtil::~DataLinkModeUtil() {}

bool DataLinkModeUtil::generateBuilderInputsForNPCMode(const std::vector<proto::DataDescription>& dsVec,
                                                       std::string& dsStringInJson)
{
    if (dsVec.size() != 2) {
        BS_LOG(ERROR, "NPC-mode only support 2 data_description");
        return false;
    }
    std::vector<std::string> dsStrVec;
    dsStrVec.reserve(dsVec.size());
    auto fullDs = dsVec[0];
    fullDs["stopTimestamp"] = fullDs["swift_stop_timestamp"];
    dsStrVec.push_back(autil::legacy::ToJsonString(fullDs, true));
    auto incDs = dsVec[1];
    dsStrVec.push_back(autil::legacy::ToJsonString(incDs, true));
    dsStringInJson = autil::legacy::ToJsonString(dsStrVec, true);
    return true;
}

bool DataLinkModeUtil::addGraphParameters(const ControlConfig& controlConfig, const std::vector<std::string>& clusters,
                                          const std::vector<proto::DataDescription>& dsVec, KeyValueMap* parameters)
{
    auto& kvMap = *parameters;
    if (controlConfig.isAllClusterNeedIncProcessor(clusters)) { // compitable for admin update with old lua script
        kvMap["hasIncProcessor"] = "true";
    } else if (!controlConfig.isAnyClusterNeedIncProcessor(clusters)) {
        kvMap["hasIncProcessor"] = "false";
    } else {
        kvMap["fullBuildProcessLastSwiftSrc"] = "false";
    }
    KeyValueMap clusterHasIncProcessorMap;
    for (auto cluster : clusters) {
        if (controlConfig.isIncProcessorExist(cluster)) {
            clusterHasIncProcessorMap[cluster] = "true";
        } else {
            clusterHasIncProcessorMap[cluster] = "false";
        }
    }
    kvMap["hasIncProcessorMap"] = autil::legacy::ToJsonString(clusterHasIncProcessorMap, true);
    if (controlConfig.getDataLinkMode() == ControlConfig::DataLinkMode::NPC_MODE) {
        string builderInputsInJson;
        if (!generateBuilderInputsForNPCMode(dsVec, builderInputsInJson)) {
            BS_LOG(ERROR, "parse builder inputs failed for npc_mode");
            return false;
        }
        kvMap["builderInputs"] = builderInputsInJson;
        //        kvMap[config::DATA_LINK_MODE] = ControlConfig::dataLinkModeToStr(controlConfig.getDataLinkMode());
    }
    return true;
}

autil::legacy::json::JsonMap
DataLinkModeUtil::generateRealTimeInfoForNormalMode(const BuildServiceConfig& buildServiceConfig,
                                                    const proto::BuildId& buildId)
{
    autil::legacy::json::JsonMap jsonMap;
    const string& applicationId = buildServiceConfig.getApplicationId();
    const string& swiftRoot = buildServiceConfig.getSwiftRoot(false /*isFull*/);
    jsonMap[PROCESSED_DOC_SWIFT_ROOT] = swiftRoot;
    jsonMap[PROCESSED_DOC_SWIFT_TOPIC_PREFIX] = applicationId;
    jsonMap[REALTIME_MODE] = REALTIME_SERVICE_MODE;
    jsonMap[BS_SERVER_ADDRESS] = buildServiceConfig.zkRoot;
    jsonMap[APP_NAME] = buildId.appname();
    jsonMap[DATA_TABLE_NAME] = buildId.datatable();
    return jsonMap;
}

autil::legacy::json::JsonMap DataLinkModeUtil::generateRealTimeInfoForNPCMode(
    const std::string& clusterName, const proto::DataDescription& realtimeDataDesc,
    const BuildServiceConfig& buildServiceConfig, const proto::BuildId& buildId)
{
    autil::legacy::json::JsonMap jsonMap;
    jsonMap[REALTIME_MODE] = REALTIME_SERVICE_NPC_MODE;
    for (KeyValueMap::const_iterator it = realtimeDataDesc.begin(); it != realtimeDataDesc.end(); it++) {
        jsonMap[it->first] = it->second;
    }
    jsonMap[DATA_DESCRIPTION_KEY] = autil::legacy::ToJsonString(realtimeDataDesc);
    auto iter = realtimeDataDesc.find(config::SWIFT_TOPIC_NAME);
    if (iter == realtimeDataDesc.end()) {
        AUTIL_LOG(ERROR, "npc mode need %s", config::SWIFT_TOPIC_NAME.c_str());
        return {};
    }
    jsonMap[BS_SERVER_ADDRESS] = buildServiceConfig.zkRoot;
    jsonMap[APP_NAME] = buildId.appname();
    jsonMap[DATA_TABLE_NAME] = buildId.datatable();
    return jsonMap;
}

std::string DataLinkModeUtil::generateNPCResourceName(const std::string& topicName)
{
    return "npc_mode_raw_topic_" + topicName;
}

autil::legacy::json::JsonMap DataLinkModeUtil::generateRealTimeInfoForFPINPMode(
    const ControlConfig& controlConfig, const BuildServiceConfig& buildServiceConfig, const std::string& clusterName,
    const proto::DataDescription& realtimeDataDesc, const proto::BuildId& buildId)
{
    if (controlConfig.isIncProcessorExist(clusterName)) {
        return generateRealTimeInfoForNormalMode(buildServiceConfig, buildId);
    }
    auto it = realtimeDataDesc.find(READ_SRC_TYPE);
    if (it == realtimeDataDesc.end()) {
        BS_LOG(ERROR, "no %s soure type in realtimeDataDescription, cannot serialize realtimeInfo",
               READ_SRC_TYPE.c_str());
        return {};
    }
    it = realtimeDataDesc.find(SWIFT_STOP_TIMESTAMP);
    if (it != realtimeDataDesc.end()) {
        BS_LOG(ERROR, "invalid key[%s] in swift type realtimeDataDescription, cannot serialize realtimeInfo",
               SWIFT_STOP_TIMESTAMP.c_str());
        return {};
    }
    autil::legacy::json::JsonMap jsonMap;
    jsonMap[REALTIME_MODE] = REALTIME_SERVICE_RAWDOC_RT_BUILD_MODE;
    for (KeyValueMap::const_iterator it = realtimeDataDesc.begin(); it != realtimeDataDesc.end(); it++) {
        jsonMap[it->first] = it->second;
    }
    jsonMap[DATA_DESCRIPTION_KEY] = autil::legacy::ToJsonString(realtimeDataDesc);
    jsonMap[BS_SERVER_ADDRESS] = buildServiceConfig.zkRoot;
    jsonMap[APP_NAME] = buildId.appname();
    jsonMap[DATA_TABLE_NAME] = buildId.datatable();
    return jsonMap;
}

autil::legacy::json::JsonMap DataLinkModeUtil::generateRealtimeInfoContent(
    const ControlConfig& controlConfig, const BuildServiceConfig& buildServiceConfig, const std::string& clusterName,
    const proto::DataDescription& realtimeDataDesc, const proto::BuildId& buildId)
{
    auto dataLinkMode = controlConfig.getDataLinkMode();
    switch (dataLinkMode) {
    case ControlConfig::DataLinkMode::NORMAL_MODE:
        return generateRealTimeInfoForNormalMode(buildServiceConfig, buildId);
    case ControlConfig::DataLinkMode::NPC_MODE:
        return generateRealTimeInfoForNPCMode(clusterName, realtimeDataDesc, buildServiceConfig, buildId);
    case ControlConfig::DataLinkMode::FP_INP_MODE:
        return generateRealTimeInfoForFPINPMode(controlConfig, buildServiceConfig, clusterName, realtimeDataDesc,
                                                buildId);
    }
    return {};
}

bool DataLinkModeUtil::addDataLinkModeParamToBuilderTarget(const ControlConfig& controlConfig,
                                                           const std::string& clusterName,
                                                           proto::DataDescription* dataDesc)
{
    auto& ds = *dataDesc;
    auto dataLinkMode = controlConfig.getTransferedDataLinkMode(clusterName);
    ds[config::DATA_LINK_MODE] = ControlConfig::dataLinkModeToStr(dataLinkMode);

    if (dataLinkMode == ControlConfig::DataLinkMode::NPC_MODE) {
        ds[config::SRC_SIGNATURE] = autil::StringUtil::toString(SwiftTopicConfig::INC_TOPIC_SRC_SIGNATURE);
        auto topicIter = ds.find(config::SWIFT_TOPIC_NAME);
        if (topicIter != ds.end()) {
            ds["name"] = DataLinkModeUtil::generateNPCResourceName(topicIter->second);
        } else {
            AUTIL_LOG(ERROR, "npc mode need %s", config::SWIFT_TOPIC_NAME.c_str());
            return false;
        }
    }
    return true;
}

bool DataLinkModeUtil::isDataLinkNPCMode(const KeyValueMap& kvMap)
{
    auto it = kvMap.find(REALTIME_MODE);
    if (it != kvMap.end()) {
        // for online realtime builder
        return it->second == REALTIME_SERVICE_NPC_MODE;
    } else {
        // for offline builder
        it = kvMap.find(config::DATA_LINK_MODE);
        if (it != kvMap.end()) {
            config::ControlConfig::DataLinkMode dataLinkMode = config::ControlConfig::DataLinkMode::NORMAL_MODE;
            if (!config::ControlConfig::parseDataLinkStr(it->second, dataLinkMode)) {
                return false;
            }
            return dataLinkMode == config::ControlConfig::DataLinkMode::NPC_MODE;
        }
        return false;
    }
    return false;
}

bool DataLinkModeUtil::validateRealtimeMode(const std::string& expectedRtMode, const KeyValueMap& kvMap)
{
    auto it = kvMap.find(REALTIME_MODE);
    if (it == kvMap.end()) {
        BS_LOG(ERROR, "ill-formed realtimeInfo, key[realtime_mode] not found");
        return false;
    }
    if (it->second != expectedRtMode) {
        BS_LOG(ERROR, "key[realtime_mode] = [%s], expects[%s]", it->second.c_str(), expectedRtMode.c_str());
        return false;
    }
    return true;
}

bool DataLinkModeUtil::adaptsRealtimeInfoToDataLinkMode(const std::string& specifiedDataLinkMode, KeyValueMap& kvMap)
{
    if (specifiedDataLinkMode.empty()) {
        return true;
    }
    config::ControlConfig::DataLinkMode targetDataLinkMode = config::ControlConfig::DataLinkMode::NORMAL_MODE;
    if (!config::ControlConfig::parseDataLinkStr(specifiedDataLinkMode, targetDataLinkMode)) {
        BS_LOG(ERROR, "invalid data_link_mode[%s]", specifiedDataLinkMode.c_str());
        return false;
    }
    switch (targetDataLinkMode) {
    case ControlConfig::DataLinkMode::NORMAL_MODE:
        return validateRealtimeMode(REALTIME_SERVICE_MODE, kvMap);
    case ControlConfig::DataLinkMode::FP_INP_MODE:
        return validateRealtimeMode(REALTIME_SERVICE_RAWDOC_RT_BUILD_MODE, kvMap);
    case ControlConfig::DataLinkMode::NPC_MODE:
        auto it = kvMap.find(REALTIME_MODE);
        if (it == kvMap.end()) {
            BS_LOG(ERROR, "ill-formed realtimeInfo, key[realtime_mode] not found");
            return false;
        }
        if (it->second == REALTIME_SERVICE_RAWDOC_RT_BUILD_MODE) {
            kvMap[REALTIME_MODE] = REALTIME_SERVICE_NPC_MODE;
            BS_LOG(INFO, "adapts legacy REALTIME_SERVICE_RAWDOC_RT_BUILD_MODE to REALTIME_SERVICE_NPC_MODE done");
            return true;
        } else if (it->second == REALTIME_SERVICE_NPC_MODE) {
            BS_LOG(INFO, "realtime mode REALTIME_SERVICE_NPC_MODE unchanged.");
            return true;
        } else {
            BS_LOG(ERROR, "cannot adapts legacy mode[%s] to REALTIME_SERVICE_NPC_MODE", it->second.c_str());
            return false;
        }
    };
    return true;
}

}} // namespace build_service::config
