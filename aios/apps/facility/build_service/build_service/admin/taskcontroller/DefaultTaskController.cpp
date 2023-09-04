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
#include "build_service/admin/taskcontroller/DefaultTaskController.h"

#include "build_service/config/ConfigReaderAccessor.h"
#include "build_service/config/TaskConfig.h"

using namespace std;
using namespace build_service::config;
using namespace autil;
using namespace autil::legacy;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, DefaultTaskController);

const string DefaultTaskController::DEFAULT_TARGET_NAME = "finish_task";

DefaultTaskController::DefaultTaskController(const DefaultTaskController& other)
    : TaskController(other)
    , _clusterName(other._clusterName)
    , _taskConfigFilePath(other._taskConfigFilePath)
    , _targets(other._targets)
    , _currentTargetIdx(other._currentTargetIdx)
    , _partitionCount(other._partitionCount)
    , _parallelNum(other._parallelNum)
{
}

DefaultTaskController::~DefaultTaskController() {}

bool DefaultTaskController::init(const std::string& clusterName, const std::string& taskConfigFilePath,
                                 const std::string& initParam)
{
    ConfigReaderAccessorPtr readerAccessor;
    _resourceManager->getResource(readerAccessor);
    ResourceReaderPtr resourceReader = readerAccessor->getLatestConfig();
    _configPath = resourceReader->getOriginalConfigPath();
    _clusterName = clusterName;
    _taskConfigFilePath = taskConfigFilePath;
    return doInit(clusterName, taskConfigFilePath, initParam);
}

bool DefaultTaskController::doInit(const std::string& clusterName, const std::string& taskConfigFilePath,
                                   const std::string& initParam)
{
    config::TaskConfig taskConfig;
    if (!getTaskConfig(taskConfigFilePath, _taskName, taskConfig)) {
        return false;
    }

    _partitionCount = taskConfig.getPartitionCount();
    _parallelNum = taskConfig.getParallelNum();
    if (_partitionCount * _parallelNum == 0) {
        BS_LOG(ERROR, "partition count [%d] or parallel num [%d] is zero", _partitionCount, _parallelNum);
        return false;
    }

    auto& controllerConfig = taskConfig.getTaskControllerConfig();
    assert(controllerConfig.getControllerType() == TaskControllerConfig::CT_BUILDIN);
    _targets = controllerConfig.getTargetConfigs();
    if (_targets.size() == 0) {
        TaskTarget target(DEFAULT_TARGET_NAME);
        target.setPartitionCount(_partitionCount);
        target.setParallelNum(_parallelNum);
        target.addTaskConfigPath(_taskConfigFilePath);
        _targets.push_back(target);
        return true;
    }

    for (size_t i = 0; i < _targets.size(); i++) {
        if (_targets[i].getPartitionCount() == 0) {
            _targets[i].setPartitionCount(_partitionCount);
        }
        if (_targets[i].getParallelNum() == 0) {
            _targets[i].setParallelNum(_parallelNum);
        }
        _targets[i].addTaskConfigPath(_taskConfigFilePath);
    }
    return true;
}

bool DefaultTaskController::operate(TaskController::Nodes& nodes)
{
    if (_currentTargetIdx == _targets.size()) {
        nodes.clear();
        return true;
    }

    if (nodes.size() == 0) {
        uint32_t nodeNum = getPartitionCount() * getParallelNum();
        nodes.reserve(nodeNum);
        for (uint32_t i = 0; i < nodeNum; ++i) {
            TaskController::Node node;
            node.nodeId = i;
            node.instanceIdx = i;
            node.taskTarget = _targets[_currentTargetIdx];
            nodes.push_back(node);
        }
        return false;
    }
    bool isFinished = true;
    for (uint32_t i = 0; i < nodes.size(); ++i) {
        if (!nodes[i].reachedTarget) {
            isFinished = false;
        }
    }
    if (!isFinished) {
        return false;
    }
    nodes.clear();
    _currentTargetIdx++;

    if (_currentTargetIdx == _targets.size()) {
        _currentTargetIdx = 0;
        _targets.clear();
        return true;
    }
    return false;
}

bool DefaultTaskController::operator==(const DefaultTaskController& other) const
{
    return _taskName == other._taskName && _targets == other._targets && _currentTargetIdx == other._currentTargetIdx &&
           _partitionCount == other._partitionCount && _parallelNum == other._parallelNum;
}

bool DefaultTaskController::operator!=(const DefaultTaskController& other) const { return !(*this == other); }

TaskController* DefaultTaskController::clone() { return new DefaultTaskController(*this); }

void DefaultTaskController::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("task_config_file_path", _taskConfigFilePath, _taskConfigFilePath);
    json.Jsonize("task_name", _taskName, _taskName);
    json.Jsonize("task_id", _taskId, _taskId);
    json.Jsonize("targets", _targets, _targets);
    json.Jsonize("current_target", _currentTargetIdx, _currentTargetIdx);
    json.Jsonize("partition_count", _partitionCount, _partitionCount);
    json.Jsonize("parallel_num", _parallelNum, _parallelNum);
    json.Jsonize("cluster_name", _clusterName, _clusterName);
    json.Jsonize("config_path", _configPath, _configPath);
    json.Jsonize("property_map", _propertyMap, _propertyMap);
}

void DefaultTaskController::supplementLableInfo(KeyValueMap& info) const
{
    info["parallel_num"] = StringUtil::toString(_parallelNum);
    info["partition_count"] = StringUtil::toString(_partitionCount);
    info["current_target_idx"] = StringUtil::toString(_currentTargetIdx);
    info["target_size"] = StringUtil::toString(_targets.size());
}

bool DefaultTaskController::updateConfig()
{
    ConfigReaderAccessorPtr readerAccessor;
    _resourceManager->getResource(readerAccessor);
    ResourceReaderPtr resourceReader = readerAccessor->getLatestConfig();
    config::TaskConfig taskConfig;
    string validConfigPath;
    if (!TaskConfig::getValidTaskConfigPath(resourceReader->getConfigPath(), "", _clusterName, _taskName,
                                            validConfigPath)) {
        _configPath = resourceReader->getOriginalConfigPath();
        return true;
    }

    bool isExist;
    if (!resourceReader->getTaskConfigWithJsonPath(_configPath, _clusterName, _taskName, "", taskConfig, isExist)) {
        BS_LOG(ERROR, "read task [%s] config fail", _taskName.c_str());
        return false;
    }

    if (!isExist) {
        BS_LOG(ERROR, "read task [%s] config from [%s] fail", _taskName.c_str(),
               resourceReader->getConfigPath().c_str());
        return false;
    }
    _partitionCount = taskConfig.getPartitionCount();
    _parallelNum = taskConfig.getParallelNum();
    if (_partitionCount * _parallelNum == 0) {
        BS_LOG(ERROR, "partition count [%d] or parallel num [%d] is zero", _partitionCount, _parallelNum);
        return false;
    }

    auto& controllerConfig = taskConfig.getTaskControllerConfig();
    std::vector<config::TaskTarget> targets = controllerConfig.getTargetConfigs();

    if (targets.size() == 0) {
        if (_targets.size() > 1 || (_targets[0] != TaskTarget(DEFAULT_TARGET_NAME))) {
            BS_LOG(ERROR, "not support update targets");
            return false;
        }
    } else {
        if (targets != _targets) {
            BS_LOG(ERROR, "not support update targets");
            return false;
        }
    }
    _configPath = resourceReader->getOriginalConfigPath();
    return true;
}

string DefaultTaskController::getUsingConfigPath() const { return _configPath; }

}} // namespace build_service::admin
