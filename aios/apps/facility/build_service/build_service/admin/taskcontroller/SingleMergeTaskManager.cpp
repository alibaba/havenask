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
#include "build_service/admin/taskcontroller/SingleMergeTaskManager.h"

#include "build_service/admin/taskcontroller/MergeCrontab.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/config/ConfigReaderAccessor.h"
#include "build_service/config/IndexPartitionOptionsWrapper.h"
#include "build_service/config/ResourceReaderManager.h"
#include "indexlib/index_define.h"

using namespace std;
using namespace autil::legacy;
using namespace build_service::config;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, SingleMergeTaskManager);

SingleMergeTaskManager::SingleMergeTaskManager(const TaskResourceManagerPtr& resourceManager)
    : _hasMergingTask(false)
    , _resourceManager(resourceManager)
{
}

SingleMergeTaskManager::~SingleMergeTaskManager() {}

void SingleMergeTaskManager::Jsonize(Jsonizable::JsonWrapper& json)
{
    json.Jsonize("config_path", _configPath, _configPath);
    json.Jsonize("data_table", _dataTable, _dataTable);
    json.Jsonize("full_merge_config", _fullMergeConfig, _fullMergeConfig);
    json.Jsonize("merging_task", _mergingTask, _mergingTask);
    json.Jsonize("pending_tasks", _pendingTasks, _pendingTasks);
    json.Jsonize("merge_crontab", _mergeCrontab, _mergeCrontab);
    json.Jsonize("cluster_name", _clusterName, _clusterName);
    json.Jsonize("has_merging_task", _hasMergingTask, _hasMergingTask);
}

void SingleMergeTaskManager::stopMergeCrontab() { _mergeCrontab.reset(); }

void SingleMergeTaskManager::triggerFullMerge()
{
    BS_LOG(INFO, "move full merge[%s] to pending for cluster[%s]", _fullMergeConfig.c_str(), _clusterName.c_str());
    _pendingTasks.push_back(_fullMergeConfig);
}

bool SingleMergeTaskManager::generateMergeTask(string& mergeTask)
{
    if (_hasMergingTask) {
        return false;
    }
    if (_pendingTasks.empty() && _mergeCrontab) {
        vector<string> mergeTasks = _mergeCrontab->generateMergeTasks();
        _pendingTasks.insert(_pendingTasks.end(), mergeTasks.begin(), mergeTasks.end());
    }
    if (_pendingTasks.empty()) {
        return false;
    }
    mergeTask = _pendingTasks.front();
    _pendingTasks.pop_front();
    _mergingTask = mergeTask;
    _hasMergingTask = true;
    BS_LOG(INFO, "generate merge task[%s] for %s", mergeTask.c_str(), _clusterName.c_str());
    return true;
}

bool SingleMergeTaskManager::operator==(const SingleMergeTaskManager& other) const
{
    bool ret = _clusterName == other._clusterName && _dataTable == other._dataTable &&
               _configPath == other._configPath && _mergingTask == other._mergingTask &&
               _pendingTasks == other._pendingTasks && _hasMergingTask == other._hasMergingTask;

    if (!ret) {
        return false;
    }
    if (_mergeCrontab && other._mergeCrontab) {
        return *_mergeCrontab == *other._mergeCrontab;
    } else if (!_mergeCrontab && !other._mergeCrontab) {
        return true;
    }
    return false;
}

bool SingleMergeTaskManager::startMergeCrontab(bool restart)
{
    if (!restart && _mergeCrontab) {
        return true;
    }
    _mergeCrontab.reset(new MergeCrontab);
    if (!_mergeCrontab->start(getConfigReader(), _clusterName)) {
        string errorMsg = "start merge crontab for cluster[" + _clusterName + "] failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}

config::ResourceReaderPtr SingleMergeTaskManager::getConfigReader()
{
    config::ConfigReaderAccessorPtr readerAccessor;
    _resourceManager->getResource(readerAccessor);
    return readerAccessor->getConfig(_configPath);
}

bool SingleMergeTaskManager::loadFromConfig(const string& dataTable, const string& clusterName,
                                            const std::string& configPath, bool restartCrontab)
{
    _clusterName = clusterName;
    _configPath = configPath;
    _dataTable = dataTable;
    ResourceReaderPtr resourceReader = getConfigReader();
    if (!IndexPartitionOptionsWrapper::getFullMergeConfigName(resourceReader.get(), _dataTable, _clusterName,
                                                              _fullMergeConfig)) {
        return false;
    }
    if (!checkMergeConfig(*(resourceReader.get()), _clusterName)) {
        return false;
    }

    if (restartCrontab) {
        return startMergeCrontab(true);
    }
    return true;
}

bool SingleMergeTaskManager::checkMergeConfig(ResourceReader& resourceReader, const string& clusterName)
{
    OfflineIndexConfigMap configMap;
    string clusterConfig = resourceReader.getClusterConfRelativePath(clusterName);
    if (!resourceReader.getConfigWithJsonPath(clusterConfig, "offline_index_config", configMap)) {
        string errorMsg = "read offline_index_config from " + clusterConfig + " failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    // check if all mergeConfigName is exist
    if (_mergingTask != ALIGN_VERSION_MERGE_STRATEGY_STR && configMap.find(_mergingTask) == configMap.end()) {
        _mergingTask = "";
        _hasMergingTask = false;
    }
    deque<string> newTasks;
    for (auto pendingTask : _pendingTasks) {
        if (pendingTask == ALIGN_VERSION_MERGE_STRATEGY_STR || configMap.find(pendingTask) != configMap.end()) {
            newTasks.push_back(pendingTask);
        }
    }
    _pendingTasks = newTasks;

    auto iter = configMap.find(BS_ALTER_FIELD_MERGE_CONFIG);
    if (iter != configMap.end() && !iter->second.offlineMergeConfig.periodMergeDescription.empty()) {
        string errorMsg = "alter_field merge config can not be period merge config"
                          " config path:[" +
                          resourceReader.getConfigPath() + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}

bool SingleMergeTaskManager::createVersion(const string& mergeConfigName)
{
    if (mergeConfigName == BS_ALTER_FIELD_MERGE_CONFIG) {
        string errorMsg = "create version fail: not support alter_field";
        BS_LOG(WARN, "%s", errorMsg.c_str());
        return false;
    }
    if (!_mergeCrontab) {
        string errorMsg = "only support createVersion after full merge";
        BS_LOG(WARN, "%s", errorMsg.c_str());
        return false;
    }

    ResourceReaderPtr resourceReader = getConfigReader();
    indexlib::config::IndexPartitionOptions indexOptions;
    if (!IndexPartitionOptionsWrapper::getIndexPartitionOptions(*(resourceReader.get()), _clusterName, mergeConfigName,
                                                                indexOptions)) {
        stringstream ss;
        ss << "invalid mergeConfigName[" << mergeConfigName << "] for cluster[" << _clusterName << "]";
        string errorMsg = ss.str();
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    _pendingTasks.push_back(mergeConfigName);
    return true;
}

void SingleMergeTaskManager::syncVersion(const string& mergeConfig)
{
    if (_pendingTasks.size() <= 0) {
        BS_LOG(INFO, "sync version for %s", _clusterName.c_str());
        _pendingTasks.push_back(mergeConfig);
    }
}

vector<string> SingleMergeTaskManager::getPendingMergeTasks() const
{
    vector<string> result;
    if (_hasMergingTask) {
        result.push_back(_mergingTask);
    }
    result.insert(result.end(), _pendingTasks.begin(), _pendingTasks.end());
    for (auto& mergeConfig : result) {
        if ("" == mergeConfig) {
            mergeConfig = "__DEFAULT_MERGE_CONFIG__";
        }
    }
    return result;
}

void SingleMergeTaskManager::fillStatusForLegacy(std::map<std::string, std::string>& fullMergeConfigs,
                                                 std::map<std::string, std::string>& mergingTasks,
                                                 std::map<std::string, std::deque<std::string>>& pendingTasks,
                                                 std::map<std::string, MergeCrontabPtr>& mergeCrontabs)
{
    fullMergeConfigs[_clusterName] = _fullMergeConfig;
    pendingTasks[_clusterName] = _pendingTasks;
    if (_mergeCrontab) {
        mergeCrontabs[_clusterName] = _mergeCrontab;
    }
    if (_hasMergingTask) {
        mergingTasks[_clusterName] = _mergingTask;
    }
}

void SingleMergeTaskManager::clearMergeTask()
{
    _pendingTasks.clear();
    _hasMergingTask = false;
    _mergingTask = "";
}
}} // namespace build_service::admin
