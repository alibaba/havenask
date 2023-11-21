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
#include "build_service/admin/taskcontroller/CloneIndexTaskController.h"

#include <assert.h>
#include <iosfwd>
#include <map>
#include <memory>
#include <stdint.h>
#include <utility>

#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/admin/controlflow/KeyValueParamParser.h"
#include "build_service/admin/controlflow/ListParamParser.h"
#include "build_service/config/AgentGroupConfig.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/TaskTarget.h"

using namespace std;
using namespace autil::legacy;
using namespace autil;
using namespace build_service::config;

namespace build_service { namespace admin {
BS_LOG_SETUP(taskcontroller, CloneIndexTaskController);

std::string CloneIndexTaskController::CLONED_INDEX_LOCATOR = "cloned-index-locator";

bool CloneIndexTaskController::doInit(const string& clusterName, const string& taskConfigFilePath,
                                      const string& initParam)
{
    _isFinished = false;
    _partitionCount = 1;
    _parallelNum = 1;
    map<string, string> taskParam;
    try {
        FromJsonString(taskParam, initParam);
    } catch (const autil::legacy::ExceptionBase& e) {
        BS_LOG(ERROR, "INVALID json str[%s]", initParam.c_str());
        return false;
    }
    TaskTarget target(DEFAULT_TARGET_NAME);
    target.setPartitionCount(_partitionCount);
    target.setParallelNum(_parallelNum);

    auto iter = taskParam.find(SOURCE_INDEX_ADMIN_ADDRESS);
    if (iter == taskParam.end()) {
        BS_LOG(ERROR, "lack of [%s] for cloneIndexTask", SOURCE_INDEX_ADMIN_ADDRESS.c_str());
        return false;
    }
    target.addTargetDescription(SOURCE_INDEX_ADMIN_ADDRESS, iter->second);
    _sourceAdminAddr = iter->second;

    iter = taskParam.find(SOURCE_INDEX_BUILD_ID);
    if (iter == taskParam.end()) {
        BS_LOG(ERROR, "lack of [%s] for cloneIndexTask", SOURCE_INDEX_BUILD_ID.c_str());
        return false;
    }
    target.addTargetDescription(SOURCE_INDEX_BUILD_ID, iter->second);
    _sourceBuildId = iter->second;

    iter = taskParam.find(MADROX_ADMIN_ADDRESS);
    if (iter == taskParam.end()) {
        BS_LOG(ERROR, "lack of [%s] for cloneIndexTask", MADROX_ADMIN_ADDRESS.c_str());
        return false;
    }

    target.addTargetDescription(MADROX_ADMIN_ADDRESS, iter->second);
    _madroxAddr = iter->second;

    iter = taskParam.find("clusterNames");
    if (iter == taskParam.end()) {
        BS_LOG(ERROR, "lock of [clusterNames] for cloneIndexTask");
        return false;
    }
    if (!ListParamParser::parseFromJsonString(iter->second.c_str(), _clusterNames)) {
        BS_LOG(ERROR, "bad json clusterNames[%s] for cloneIndexTask", iter->second.c_str());
        return false;
    }
    target.addTargetDescription("clusterNames", iter->second);

    iter = taskParam.find("versionIds");
    if (iter != taskParam.end()) {
        try {
            autil::legacy::FromJsonString(_targetVersions, iter->second);
        } catch (autil::legacy::ExceptionBase& e) {
            BS_LOG(ERROR, "bad json versions[%s] for cloneIndexTask", iter->second.c_str());
            return false;
        }
        if (_targetVersions.size() != _clusterNames.size()) {
            BS_LOG(ERROR, "target version size [%lu] not equal cluster size [%lu]", _targetVersions.size(),
                   _clusterNames.size());
            return false;
        }
        target.addTargetDescription("versionIds", iter->second);
    }
    _targets.push_back(target);
    return true;
}

bool CloneIndexTaskController::start(const KeyValueMap& kvMap)
{
    if (_isFinished) {
        BS_LOG(ERROR, "CloneIndexTaskController start() is not re-accessible");
        return false;
    }
    return true;
}

bool CloneIndexTaskController::updateConfig() { return true; }

bool CloneIndexTaskController::operate(TaskController::Nodes& taskNodes)
{
    if (_isFinished) {
        taskNodes.clear();
        return true;
    }
    if (taskNodes.size() == 0) {
        TaskController::Node node;
        node.nodeId = 0;
        node.instanceIdx = 0;
        node.taskTarget = _targets[0];
        taskNodes.push_back(node);
        return false;
    }
    assert(taskNodes.size() == 1);
    if (taskNodes[0].reachedTarget) {
        const auto& statusDesc = taskNodes[0].statusDescription;
        KeyValueMap statusMap;
        if (!KeyValueParamParser::parseFromJsonString(statusDesc.c_str(), statusMap)) {
            BS_LOG(ERROR, "error parsing statusDescription[%s] in CloneIndexTask[%s]", statusDesc.c_str(),
                   _sourceBuildId.c_str());
            return false;
        }
        auto it = statusMap.find(INDEX_CLONE_LOCATOR);
        if (it == statusMap.end()) {
            BS_LOG(ERROR, "no cloned-index-locator defined in statusDescription[%s] in CloneIndexTask[%s]",
                   statusDesc.c_str(), _sourceBuildId.c_str());
            return false;
        }

        string locatorStr = it->second;

        // get locator for each cluster, and then setProperty
        KeyValueMap cluster2Locator;
        if (!KeyValueParamParser::parseFromJsonString(locatorStr.c_str(), cluster2Locator)) {
            BS_LOG(ERROR, "error parsing locators[%s] statusDescription[%s] in CloneIndexTask[%s]", locatorStr.c_str(),
                   statusDesc.c_str(), _sourceBuildId.c_str());
            return false;
        }

        if (cluster2Locator.size() != _clusterNames.size()) {
            BS_LOG(ERROR, "index locators[%s] not match clusterNames size[%zu]", locatorStr.c_str(),
                   _clusterNames.size());
            return false;
        }

        for (const auto& clusterName : _clusterNames) {
            auto iter = cluster2Locator.find(clusterName);
            if (iter == cluster2Locator.end()) {
                BS_LOG(ERROR, "missing index locator for cluster[%s] in indexLocatorMap", clusterName.c_str());
                return false;
            }
            int64_t tsValue;
            if (!autil::StringUtil::fromString(iter->second, tsValue)) {
                BS_LOG(ERROR,
                       "invalid timesatmp in cloned-index-locator[%s] defined in statusDescription[%s] "
                       "in CloneIndexTask[%s] for cluster [%s]",
                       locatorStr.c_str(), statusDesc.c_str(), _sourceBuildId.c_str(), clusterName.c_str());
                return false;
            }
            std::string locatorKey = clusterName + '-' + CLONED_INDEX_LOCATOR;
            BS_LOG(INFO, "set cluster [%s], clone locator [%s]", clusterName.c_str(), iter->second.c_str());
            SetProperty(locatorKey, iter->second);
        }
        taskNodes.clear();
        _isFinished = true;
        BS_LOG(INFO, "finish cloneIndex task [%s]", _sourceBuildId.c_str());
        return true;
    }
    return false;
}

void CloneIndexTaskController::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    DefaultTaskController::Jsonize(json);
    json.Jsonize("is_finished", _isFinished, _isFinished);
    json.Jsonize("source_build_id", _sourceBuildId, _sourceBuildId);
    json.Jsonize("source_admin_address", _sourceAdminAddr, _sourceAdminAddr);
    json.Jsonize("madrox_admin_address", _madroxAddr, _madroxAddr);
    json.Jsonize("clusterNames", _clusterNames, _clusterNames);
    json.Jsonize("versionIds", _targetVersions, _targetVersions);
}

}} // namespace build_service::admin
