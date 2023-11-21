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
#include "build_service/admin/taskcontroller/ProcessorParamParser.h"

#include <iosfwd>
#include <memory>
#include <stdint.h>

#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/common/ResourceKeeperGuard.h"
#include "build_service/common/SwiftResourceKeeper.h"
#include "build_service/config/AgentGroupConfig.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ConfigReaderAccessor.h"
#include "build_service/config/TaskOutputConfig.h"
#include "indexlib/misc/common.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace build_service::config;
using build_service::common::SwiftResourceKeeper;
using build_service::common::SwiftResourceKeeperPtr;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, ProcessorParamParser);

ProcessorParamParser::ProcessorParamParser() {}

ProcessorParamParser::~ProcessorParamParser() {}

bool ProcessorParamParser::parseOutput(const KeyValueMap& kvMap, const config::ResourceReaderPtr& resourceReader,
                                       const TaskResourceManagerPtr& resMgr)
{
    string outputStr = getValueFromKeyValueMap(kvMap, "output");
    if (_clusterNames.size() > 1 && !outputStr.empty()) {
        BS_LOG(ERROR, "not support parse output[%s] for multi cluster.", outputStr.c_str());
        return false;
        // legacy for batch mode
    }

    if (outputStr.empty()) {
        return true;
    }
    TaskOutputConfig outputConfig;
    if (!parseIOConfig(outputStr, &outputConfig)) {
        return false;
    }
    if (outputConfig.getType() != "dependResource") {
        BS_LOG(ERROR, "processor output only support swift depend resource[%s].", outputStr.c_str());
        return false;
    }
    string resourceName;
    if (!outputConfig.getParam("resourceName", &resourceName)) {
        BS_LOG(ERROR, "processor output no resource name.");
        return false;
    }
    auto resourceGuard = resMgr->applyResource("tmp_role", resourceName, resourceReader);
    if (!resourceGuard) {
        BS_LOG(ERROR, "processor output no resource [%s].", resourceName.c_str());
        return false;
    }
    SwiftResourceKeeperPtr swiftKeeper = DYNAMIC_POINTER_CAST(SwiftResourceKeeper, resourceGuard->getResourceKeeper());
    if (!swiftKeeper) {
        BS_LOG(ERROR, "processor output only support swift resource");
        return false;
    }
    outputConfig.addIdentifier(swiftKeeper->getTopicId());
    _processorOutput.addOutput(_clusterNames[0], outputConfig);
    return true;
}

bool ProcessorParamParser::parseIOConfig(const std::string& input, TaskInputConfig* inputConfig)
{
    KeyValueMap inputParams;
    try {
        FromJsonString(inputParams, input);
    } catch (const autil::legacy::ExceptionBase& e) {
        BS_LOG(ERROR, "input[%s] is invalid.", input.c_str());
        return false;
    }
    string type = getValueFromKeyValueMap(inputParams, "type");
    if (type.empty()) {
        BS_LOG(ERROR, "input[%s] is invalid, no type.", input.c_str());
        return false;
    }
    string paramStr = getValueFromKeyValueMap(inputParams, "params");
    KeyValueMap params;
    if (!paramStr.empty()) {
        try {
            FromJsonString(params, paramStr);
        } catch (const autil::legacy::ExceptionBase& e) {
            BS_LOG(ERROR, "input[%s] params is invalid.", input.c_str());
            return false;
        }
    }
    string moduleName = getValueFromKeyValueMap(inputParams, "module_name");
    *inputConfig = TaskInputConfig(type, moduleName, params);
    return true;
}

bool ProcessorParamParser::parse(const KeyValueMap& kvMap, const TaskResourceManagerPtr& resMgr)
{
    if (!_processorInput.parseInput(kvMap)) {
        return false;
    }

    // TODO: batchmode use clusterNames?
    string clusterName = getValueFromKeyValueMap(kvMap, "clusterName");
    if (clusterName.empty()) {
        BS_LOG(ERROR, "processor task init fail, no clusterName");
        return false;
    }

    ConfigReaderAccessorPtr configAccessor;
    resMgr->getResource(configAccessor);
    if (!_processorInput.isBatchMode()) {
        _clusterNames.push_back(clusterName);
        string schemaIdStr = getValueFromKeyValueMap(kvMap, "schemaId");
        if (schemaIdStr.empty()) {
            BS_LOG(ERROR, "processor no schemaId");
            return false;
        }

        int64_t schemaId;
        if (!StringUtil::fromString(schemaIdStr, schemaId)) {
            BS_LOG(ERROR, "invalid schemaId[%s]", schemaIdStr.c_str());
            return false;
        }
        auto resourceReader = configAccessor->getConfig(clusterName, schemaId);
        if (!resourceReader) {
            BS_LOG(ERROR, "processor no config");
            return false;
        }
        _configInfo.configPath = resourceReader->getOriginalConfigPath();
    } else {
        FromJsonString(_clusterNames, clusterName);
        _configInfo.configPath = configAccessor->getLatestConfigPath();
    }

    _configInfo.configName = getValueFromKeyValueMap(kvMap, READ_SRC_CONFIG);
    _configInfo.stage = getValueFromKeyValueMap(kvMap, "stage");
    _configInfo.preStage = getValueFromKeyValueMap(kvMap, "preStage");
    if (_configInfo.stage == _configInfo.preStage && !_configInfo.stage.empty()) {
        BS_LOG(ERROR, "parse processor param fail: stage [%s] preStage [%s] is same", _configInfo.stage.c_str(),
               _configInfo.preStage.c_str());
        return false;
    }
    string isLastStage = getValueFromKeyValueMap(kvMap, "isLastStage");
    _configInfo.isLastStage = isLastStage == string("true") ? true : false;
    if (_clusterNames.size() <= 0) {
        BS_LOG(ERROR, "parse processor param fail, no clusterName");
        return false;
    }
    if (!parseOutput(kvMap, configAccessor->getConfig(_configInfo.configPath), resMgr)) {
        BS_LOG(ERROR, "parse output failed");
        return false;
    }

    return true;
}

}} // namespace build_service::admin
