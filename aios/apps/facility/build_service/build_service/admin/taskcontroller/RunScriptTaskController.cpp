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
#include "build_service/admin/taskcontroller/RunScriptTaskController.h"

#include <iosfwd>
#include <map>
#include <memory>
#include <utility>
#include <vector>

#include "autil/Log.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/config/AgentGroupConfig.h"
#include "build_service/config/ConfigReaderAccessor.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/config/TaskConfig.h"
#include "build_service/config/TaskTarget.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace build_service::config;

namespace build_service { namespace admin {
BS_LOG_SETUP(taskcontroller, RunScriptTaskController);

RunScriptTaskController::RunScriptTaskController(const string& taskId, const string& taskName,
                                                 const TaskResourceManagerPtr& resMgr)
    : DefaultTaskController(taskId, taskName, resMgr)
{
    _partitionCount = 1;
    _parallelNum = 1;
}

RunScriptTaskController::~RunScriptTaskController() {}

bool RunScriptTaskController::doInit(const string& clusterName, const string& taskConfigPath, const string& initParam)
{
    if (!taskConfigPath.empty()) {
        AUTIL_LOG(WARN, "RunScriptTask do not support user defined TaskConfigPaht[%s], ignore", taskConfigPath.c_str());
    }
    return true;
}

bool RunScriptTaskController::updateConfig()
{
    ConfigReaderAccessorPtr readerAccessor;
    _resourceManager->getResource(readerAccessor);
    ResourceReaderPtr resourceReader = readerAccessor->getLatestConfig();
    _configPath = resourceReader->getOriginalConfigPath();
    return true;
}

bool RunScriptTaskController::start(const KeyValueMap& kvMap)
{
    string configFileName = "default_config.json";
    auto iter = kvMap.find("configFileName");
    if (iter == kvMap.end()) {
        AUTIL_LOG(WARN, "Param has not configFileName for run script task, use defaultConfig[default_config.json]");
    } else {
        configFileName = iter->second;
        AUTIL_LOG(INFO, "use configFile[%s] for run script task.", configFileName.c_str());
    }

    ScriptTaskConfig scriptConf;
    if (!prepareRunScriptTask(configFileName, kvMap, scriptConf)) {
        return false;
    }

    _targets.clear();
    TaskTarget target(DEFAULT_TARGET_NAME);
    target.setPartitionCount(_partitionCount);
    target.setParallelNum(_parallelNum);
    target.addTargetDescription("runScript", ToJsonString(scriptConf, true));
    _targets.push_back(target);
    return true;
}

bool RunScriptTaskController::prepareRunScriptTask(const string& configFileName, const KeyValueMap& kvMap,
                                                   ScriptTaskConfig& scriptConf)
{
    if (configFileName.empty()) {
        BS_LOG(ERROR, "cannot use empty configFileName for run script task");
        return false;
    }

    ConfigReaderAccessorPtr readerAccessor;
    _resourceManager->getResource(readerAccessor);
    ResourceReaderPtr resourceReader = readerAccessor->getLatestConfig();

    string relativePath = ResourceReader::getTaskScriptFileRelativePath(configFileName);
    string content;
    if (!resourceReader->getConfigContent(relativePath, content)) {
        BS_LOG(ERROR, "read script config file [%s] from config path [%s] fail!", relativePath.c_str(),
               resourceReader->getConfigPath().c_str());
        return false;
    }

    try {
        FromJsonString(scriptConf, content);
    } catch (const autil::legacy::ExceptionBase& e) {
        BS_LOG(ERROR, "parse config [%s] failed, exception [%s]", content.c_str(), e.what());
        return false;
    }

    auto iter = kvMap.find("replaceArguments");
    if (iter != kvMap.end()) {
        scriptConf.arguments = iter->second;
    }
    iter = kvMap.find("appendArguments");
    if (iter != kvMap.end()) {
        if (scriptConf.arguments.empty() || *scriptConf.arguments.rbegin() == ' ') {
            scriptConf.arguments += iter->second;
        } else {
            scriptConf.arguments += string(" ");
            scriptConf.arguments += iter->second;
        }
    }
    if (scriptConf.partitionCount <= 0 || scriptConf.parallelNum <= 0) {
        AUTIL_LOG(ERROR, "PartitionCount[%d] & ParallelNum[%d] are invalid.", scriptConf.partitionCount,
                  scriptConf.parallelNum);
        return false;
    }
    _partitionCount = scriptConf.partitionCount;
    _parallelNum = scriptConf.parallelNum;
    BS_LOG(INFO, "run script arguments [%s], PartitionCount[%d], ParallelNum[%d].", scriptConf.arguments.c_str(),
           _partitionCount, _parallelNum);
    return true;
}

void RunScriptTaskController::Jsonize(JsonWrapper& json) { DefaultTaskController::Jsonize(json); }

}} // namespace build_service::admin
