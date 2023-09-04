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
#pragma once

#include "build_service/common_define.h"
#include "build_service/config/BuildServiceConfig.h"
#include "build_service/config/ControlConfig.h"
#include "build_service/proto/DataDescription.h"
#include "build_service/util/Log.h"
namespace build_service { namespace config {

class DataLinkModeUtil
{
public:
    DataLinkModeUtil();
    ~DataLinkModeUtil();

    DataLinkModeUtil(const DataLinkModeUtil&) = delete;
    DataLinkModeUtil& operator=(const DataLinkModeUtil&) = delete;
    DataLinkModeUtil(DataLinkModeUtil&&) = delete;
    DataLinkModeUtil& operator=(DataLinkModeUtil&&) = delete;

public:
    static bool addGraphParameters(const ControlConfig& controlConfig, const std::vector<std::string>& clusters,
                                   const std::vector<proto::DataDescription>& dsVec, KeyValueMap* parameters);

    static std::string generateRealtimeInfoContent(const ControlConfig& controlConfig,
                                                   const BuildServiceConfig& buildServiceConfig,
                                                   const std::string& clusterName,
                                                   const proto::DataDescription& realtimeDataDesc);
    static std::string generateNPCResourceName(const std::string& topicName);

    static bool isDataLinkNPCMode(const KeyValueMap& kvMap);

    static bool validateRealtimeMode(const std::string& expectedRtMode, const KeyValueMap& kvMap);

    static bool adaptsRealtimeInfoToDataLinkMode(const std::string& specifiedDataLinkMode, KeyValueMap& kvMap);

private:
    static std::string generateRealTimeInfoForFPINPMode(const ControlConfig& controlConfig,
                                                        const BuildServiceConfig& buildServiceConfig,
                                                        const std::string& clusterName,
                                                        const proto::DataDescription& realtimeDataDesc);
    static std::string generateRealTimeInfoForNormalMode(const BuildServiceConfig& buildServiceConfig);
    static std::string generateRealTimeInfoForNPCMode(const std::string& clusterName,
                                                      const proto::DataDescription& realtimeDataDesc);
    static bool generateBuilderInputsForNPCMode(const std::vector<proto::DataDescription>& dsVec,
                                                std::string& dsStringInJson);

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(DataLinkModeUtil);

}} // namespace build_service::config
