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
#include "build_service/config/TaskConfig.h"

#include "autil/EnvUtil.h"
#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable.h"
#include "build_service/config/ResourceReader.h"
#include "fslib/util/FileUtil.h"

using namespace std;
using namespace build_service::util;
using namespace build_service::plugin;
using namespace autil::legacy;
using namespace autil;
using namespace autil::legacy::json;

namespace build_service { namespace config {
BS_LOG_SETUP(config, TaskConfig);

TaskConfig::TaskConfig() : _partitionCount(1), _parallelNum(1) {}

TaskConfig::~TaskConfig() {}

bool TaskConfig::isBuildInTask(const std::string& taskName)
{
    if (taskName == BS_REPARTITION) {
        return true;
    }

    if (taskName == BS_EXTRACT_DOC) {
        return true;
    }

    if (taskName == BS_PREPARE_DATA_SOURCE) {
        return true;
    }

    if (taskName == BS_ROLLBACK_INDEX) {
        return true;
    }

    if (taskName == BS_DROP_BUILDING_INDEX) {
        return true;
    }

    if (taskName == BS_TASK_DOC_RECLAIM) {
        return true;
    }

    if (taskName == BS_TASK_RUN_SCRIPT) {
        return true;
    }

    if (taskName == BS_TASK_END_BUILD) {
        return true;
    }

    if (taskName == BS_TASK_CLONE_INDEX) {
        return true;
    }

    if (taskName == BS_TASK_SYNC_INDEX) {
        return true;
    }

    if (taskName == BS_TASK_BATCH_CONTROL) {
        return true;
    }

    return false;
}

void TaskConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("module_infos", _moduleInfos, _moduleInfos);
    json.Jsonize("task_module_name", _taskModuleName, _taskModuleName);
    json.Jsonize("task_params", _parameters, _parameters);
    json.Jsonize("inputs", _inputConfigs, _inputConfigs);
    json.Jsonize("outputs", _outputConfigs, _outputConfigs);
    json.Jsonize("task_controller_config", _taskControllerConfig, _taskControllerConfig);
    json.Jsonize("partition_count", _partitionCount, _partitionCount);
    json.Jsonize("parallel_num", _parallelNum, _parallelNum);
}

bool TaskConfig::prepareBuildServiceTaskModuleInfo(const ResourceReaderPtr& resourceReader, std::string& pluginPath)
{
    string env = autil::EnvUtil::getEnv("LD_LIBRARY_PATH");
    string paths;
    if (env.empty()) {
        paths = resourceReader->getPluginPath() + ":./";
    } else {
        paths = resourceReader->getPluginPath() + ":./:" + string(env);
    }

    AUTIL_LOG(INFO, "scan path: [%s]", paths.c_str());
    StringUtil::trim(paths);
    StringTokenizer tokens(paths, ":", StringTokenizer::TOKEN_TRIM);
    StringTokenizer::Iterator it = tokens.begin();
    while (it != tokens.end()) {
        if (parseSoUnderOnePath((*it), pluginPath)) {
            AUTIL_LOG(INFO, "find libbuild_service_task.so under [%s]", pluginPath.c_str());
            return true;
        }
        it++;
    }
    return false;
}

bool TaskConfig::parseSoUnderOnePath(const std::string& path, std::string& pluginPath)
{
    vector<string> fileList;
    if (!fslib::util::FileUtil::listDir(path, fileList)) {
        return false;
    }
    for (auto& filePath : fileList) {
        if (filePath == "libbuild_service_tasks.so") {
            pluginPath = path;
            ModuleInfo moduleInfo;
            moduleInfo.moduleName = "build_service_task";
            moduleInfo.modulePath = "libbuild_service_tasks.so";
            _moduleInfos.push_back(moduleInfo);
            _taskModuleName = "build_service_task";
            return true;
        }
    }
    return false;
}

bool TaskConfig::getValidTaskConfigPath(const std::string& configPath, const std::string& taskConfigFilePath,
                                        const std::string& clusterName, const std::string& taskName,
                                        std::string& validTaskConfigPath)
{
    if (!taskConfigFilePath.empty()) {
        validTaskConfigPath = taskConfigFilePath;
        return true;
    }

    string taskFilePath = fslib::util::FileUtil::joinFilePath(
        configPath, ResourceReader::getTaskConfRelativePath(configPath, clusterName, taskName));

    bool exist;
    if (!fslib::util::FileUtil::isFile(taskFilePath, exist)) {
        BS_LOG(ERROR, "fail to judge task config file exist");
        return false;
    }
    if (exist) {
        validTaskConfigPath = taskFilePath;
        return true;
    }
    validTaskConfigPath.clear();
    return true;
}

bool TaskConfig::loadFromFile(const std::string& filePath, TaskConfig& taskConfig)
{
    string content;
    if (!fslib::util::FileUtil::readFile(filePath, content)) {
        BS_LOG(ERROR, "read task config file[%s] failed", filePath.c_str());
        return false;
    }
    try {
        FromJsonString(taskConfig, content);
    } catch (const ExceptionBase& e) {
        string errorMsg = "jsonize " + content + " failed, exception: " + string(e.what());
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}

bool TaskConfig::getInputConfig(const string& name, TaskInputConfig& input) const
{
    auto iter = _inputConfigs.find(name);
    if (iter != _inputConfigs.end()) {
        input = iter->second;
        return true;
    }
    return false;
}
bool TaskConfig::getOutputConfig(const string& name, TaskOutputConfig& output) const
{
    auto iter = _outputConfigs.find(name);
    if (iter != _outputConfigs.end()) {
        output = iter->second;
        return true;
    }
    return false;
}
}} // namespace build_service::config
