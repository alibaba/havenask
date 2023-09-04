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
#include "build_service/admin/taskcontroller/ProcessorInput.h"

#include "build_service/admin/taskcontroller/ProcessorParamParser.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/util/DataSourceHelper.h"

using autil::StringUtil;
using autil::legacy::FromJsonString;
using build_service::admin::ProcessorParamParser;
using build_service::config::INPUT_CONFIG;
using build_service::config::READ_SRC;
using build_service::config::READ_SRC_CONFIG;
using build_service::config::READ_SRC_TYPE;
using build_service::config::SRC_BATCH_MODE;
using build_service::config::TaskInputConfig;
using build_service::proto::DataDescription;
using std::string;
using std::stringstream;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, ProcessorInput);

ProcessorInput::~ProcessorInput() {}

bool ProcessorInput::isBatchMode(proto::DataDescriptions& _dataDescriptions)
{
    int32_t lastDs = _dataDescriptions.size() - 1;
    if (lastDs >= 0 && util::DataSourceHelper::isRealtime(_dataDescriptions[lastDs]) &&
        _dataDescriptions[lastDs].find(SRC_BATCH_MODE) != _dataDescriptions[lastDs].end() &&
        _dataDescriptions[lastDs][SRC_BATCH_MODE] == "true") {
        return true;
    }
    return false;
}

bool ProcessorInput::parseDataDescriptions(const std::string& dataSource)
{
    try {
        FromJsonString(dataDescriptions.toVector(), dataSource);
    } catch (const autil::legacy::ExceptionBase& e) {
        stringstream ss;
        ss << "parse data descriptions [" << dataSource << "] failed, exception[" << string(e.what()) << "]";
        BS_LOG(ERROR, "%s", ss.str().c_str());
        return false;
    }
    return true;
}

bool ProcessorInput::fillDataDescriptions(const KeyValueMap& kvMap)
{
    string dataSource = getValueFromKeyValueMap(kvMap, "dataDescriptions");
    if (dataSource.empty()) {
        BS_LOG(ERROR, "processor no datasource");
        return false;
    }
    if (!parseDataDescriptions(dataSource)) {
        return false;
    }
    return true;
}

bool ProcessorInput::supportUpdateCheckpoint()
{
    if (util::DataSourceHelper::isRealtime(dataDescriptions[src]) ||
        INPUT_CONFIG == dataDescriptions[src][READ_SRC_TYPE]) {
        return true;
    }
    return false;
}

bool ProcessorInput::supportUpdatePartitionCount()
{
    if (util::DataSourceHelper::isRealtime(dataDescriptions[src])) {
        return true;
    }
    // todo: judge by input config?
    if (dataDescriptions[src][READ_SRC_TYPE] == INPUT_CONFIG) {
        return true;
    }
    return false;
}

bool ProcessorInput::parseInput(const KeyValueMap& kvMap)
{
    string input = getValueFromKeyValueMap(kvMap, "input");
    if (input.empty()) {
        if (!fillDataDescriptions(kvMap)) {
            return false;
        }
    } else {
        TaskInputConfig inputConfig;
        if (!ProcessorParamParser::parseIOConfig(input, &inputConfig)) {
            BS_LOG(ERROR, "input[%s] is invalid.", input.c_str());
            return false;
        }
        if (inputConfig.getType() == "dataDescriptions") {
            auto params = inputConfig.getParameters();
            if (!fillDataDescriptions(params)) {
                return false;
            }
        } else {
            DataDescription dataDescription;
            dataDescription[READ_SRC] = INPUT_CONFIG;
            dataDescription[READ_SRC_CONFIG] = ToJsonString(inputConfig);
            dataDescription[READ_SRC_TYPE] = INPUT_CONFIG;
            dataDescriptions.push_back(dataDescription);
        }
    }

    string checkpointStr = getValueFromKeyValueMap(kvMap, "checkpoint");
    if (!checkpointStr.empty()) {
        if (!StringUtil::fromString(checkpointStr, offset)) {
            BS_LOG(ERROR, "invalid checkpoint[%s]", checkpointStr.c_str());
            return false;
        }
    }

    string dsIdStr = getValueFromKeyValueMap(kvMap, "startDataDescriptionId");
    if (!dsIdStr.empty()) {
        if (!StringUtil::fromString(dsIdStr, src)) {
            BS_LOG(ERROR, "invalid startDataDescriptionId[%s]", dsIdStr.c_str());
            return false;
        }
    }

    string fullBuildSwiftStr = getValueFromKeyValueMap(kvMap, "fullBuildProcessLastSwiftSrc");
    if (!fullBuildSwiftStr.empty()) {
        if (!StringUtil::fromString(fullBuildSwiftStr, fullBuildProcessLastSwiftSrc)) {
            BS_LOG(ERROR, "invalid fullBuildSwiftStr[%s]", fullBuildSwiftStr.c_str());
            return false;
        }
    }

    if (src != -1 && offset == -1) {
        BS_LOG(ERROR, "startDataDescriptionId [%s] need use with checkpoint", dsIdStr.c_str());
        return false;
    }

    string batchMaskStr = getValueFromKeyValueMap(kvMap, "batchMask");
    if (!batchMaskStr.empty()) {
        if (!StringUtil::fromString(batchMaskStr, batchMask)) {
            batchMask = -1;
        }
    }
    startCheckpointName = getValueFromKeyValueMap(kvMap, "startCheckpointName");
    if (!startCheckpointName.empty() && src != -1) {
        BS_LOG(ERROR, "startDataDescriptionId [%s] not support use with startCheckpointName [%s]", dsIdStr.c_str(),
               startCheckpointName.c_str());
        return false;
    }

    rawDocQueryString = getValueFromKeyValueMap(kvMap, "rawDocQuery");
    return true;
}

}} // namespace build_service::admin
