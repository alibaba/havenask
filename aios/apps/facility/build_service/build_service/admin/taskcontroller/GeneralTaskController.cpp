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
#include "build_service/admin/taskcontroller/GeneralTaskController.h"

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "build_service/common/BeeperCollectorDefine.h"
#include "build_service/config/BuildRuleConfig.h"
#include "build_service/config/BuildServiceConfig.h"
#include "build_service/config/ConfigReaderAccessor.h"
#include "build_service/config/TaskConfig.h"
#include "build_service/util/IndexPathConstructor.h"
#include "build_service/util/RangeUtil.h"
#include "fslib/util/FileUtil.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/PathUtil.h"
#include "indexlib/config/IndexTaskConfig.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/util/EpochIdUtil.h"
#include "indexlib/util/PathUtil.h"

namespace build_service::admin {
BS_LOG_SETUP(admin, GeneralTaskController);

GeneralTaskController::GeneralTaskController(const GeneralTaskController& other)
    : TaskController(other)
    , _clusterName(other._clusterName)
    , _taskConfigFilePath(other._taskConfigFilePath)
    , _configPath(other._configPath)
    , _parallelNum(other._parallelNum)
    , _threadNum(other._threadNum)
    , _taskParam(other._taskParam)
{
    _taskInfo = other._taskInfo;
    _partitionTask = other._partitionTask;
}

std::string GeneralTaskController::getRoleName(const std::string& taskName, const std::string& taskType)
{
    if (taskType != "merge") {
        return taskType;
    }
    if (taskName == "full_merge") {
        return "fullMerge";
    }
    return "incMerge";
}

bool GeneralTaskController::initParallelAndThreadNum(const config::ResourceReaderPtr& resourceReader,
                                                     const std::string& clusterName, const std::string& planTaskName,
                                                     const std::string& planTaskType)
{
    auto tabletOptions = resourceReader->getTabletOptions(clusterName);
    if (!tabletOptions) {
        AUTIL_LOG(ERROR, "get tablet options failed");
        return false;
    }
    auto taskConfig = tabletOptions->GetIndexTaskConfig(planTaskType, planTaskName);
    if (taskConfig != std::nullopt) {
        auto [status, num] = taskConfig->GetSetting<uint32_t>(config::BS_TASK_PARALLEL_NUM);
        if (status.IsOK()) {
            _parallelNum = num;
            std::tie(status, num) = taskConfig->GetSetting<uint32_t>(config::BS_TASK_THREAD_NUM);
            if (status.IsOK()) {
                _threadNum = num;
            }
            return true;
        }
    }
    config::TaskConfig generalTaskConfig;
    if (!getTaskConfig(_taskConfigFilePath, _taskName, generalTaskConfig)) {
        // when user not set general_task_task.json, will use default parallel 1
        return true;
    }
    _parallelNum = generalTaskConfig.getParallelNum();
    return true;
}

bool GeneralTaskController::init(const std::string& clusterName, const std::string& taskConfigFilePath,
                                 const std::string& initParam)
{
    config::ConfigReaderAccessorPtr readerAccessor;
    _resourceManager->getResource(readerAccessor);
    config::ResourceReaderPtr resourceReader = readerAccessor->getLatestConfig();
    _configPath = resourceReader->getOriginalConfigPath();
    _clusterName = clusterName;
    _taskConfigFilePath = taskConfigFilePath;
    GeneralTaskParam param;
    try {
        autil::legacy::FromJsonString(param, initParam);
    } catch (const autil::legacy::ExceptionBase& e) {
        std::stringstream ss;
        ss << "invalid general task param:" << initParam;
        BS_LOG(ERROR, "%s", ss.str().c_str());
        return false;
    }
    std::string taskName = param.plan.get().taskname();
    std::string taskType = param.plan.get().tasktype();
    _roleName = getRoleName(taskName, taskType);
    if (!initParallelAndThreadNum(resourceReader, _clusterName, taskName, taskType)) {
        BS_LOG(ERROR, "init parallel and thread num failed");
        return false;
    }

    if (!initTaskManager(param, &(param.plan.get()))) {
        BS_LOG(ERROR, "init task manager failed");
        return false;
    }
    _taskParam = std::move(param);
    return true;
}

bool GeneralTaskController::initTaskManager(const GeneralTaskParam& param, const proto::OperationPlan* plan)
{
    auto singleTaskManager = std::make_shared<SingleGeneralTaskManager>(
        /*id*/ param.taskEpochId, _resourceManager);
    KeyValueMap partitionParam;
    partitionParam["branch_id"] = param.branchId;
    auto taskWorkRoot = indexlibv2::PathUtil::GetTaskTempWorkRoot(param.partitionIndexRoot, param.taskEpochId);
    if (taskWorkRoot.empty()) {
        BS_LOG(ERROR, "get task work root failed");
        return false;
    }
    partitionParam["task_work_root"] = taskWorkRoot;
    if (param.sourceVersionIds.empty()) {
        BS_LOG(ERROR, "source version unspecified");
        return false;
    }
    partitionParam["base_version_id"] = std::to_string(param.sourceVersionIds[0]);
    partitionParam["cluster_name"] = _clusterName;
    if (!singleTaskManager->init(partitionParam, plan)) {
        BS_LOG(ERROR, "init single task manager failed");
        return false;
    }
    _partitionTask = std::move(singleTaskManager);
    return true;
}

bool GeneralTaskController::operate(TaskController::Nodes& nodes)
{
    {
        std::lock_guard<std::mutex> lock(_mutex);
        if (!_taskInfo.empty()) {
            return true;
        }
    }
    TaskController::Nodes tmpNodes = nodes;
    bool ret = _partitionTask->operate(&tmpNodes, _parallelNum, _threadNum);
    for (auto& node : tmpNodes) {
        addDescription(_taskParam, &node);
        node.roleName = _roleName;
    }
    nodes = std::move(tmpNodes);
    if (ret) {
        // task finished
        std::lock_guard<std::mutex> lock(_mutex);
        _taskInfo = _partitionTask->getTaskInfo();
    }
    return ret;
}

void GeneralTaskController::addDescription(const GeneralTaskParam& param, TaskController::Node* node)
{
    if (node->taskTarget.hasTargetDescription("task_description_resolved")) {
        return;
    }
    node->taskTarget.setPartitionCount(1);
    node->taskTarget.setParallelNum(_parallelNum);
    node->taskTarget.addTargetDescription(config::BS_GENERAL_TASK_EPOCH_ID, _taskParam.taskEpochId);
    node->taskTarget.addTargetDescription(config::BS_GENERAL_TASK_PARTITION_INDEX_ROOT, _taskParam.partitionIndexRoot);
    std::vector<std::pair</*partitionRoot*/ std::string, indexlibv2::versionid_t>> srcVersions;
    auto sourceRoot = _taskParam.partitionIndexRoot;
    auto iter = _taskParam.params.find(config::BS_GENERAL_TASK_SOURCE_ROOT);
    if (iter != _taskParam.params.end()) {
        sourceRoot = iter->second;
    }
    auto reservedVersionIter = _taskParam.params.find(indexlibv2::table::RESERVED_VERSION_SET);
    if (reservedVersionIter != _taskParam.params.end()) {
        node->taskTarget.addTargetDescription(indexlibv2::table::RESERVED_VERSION_SET, reservedVersionIter->second);
    }
    auto reservedVersionCoordIter = _taskParam.params.find(indexlibv2::table::RESERVED_VERSION_COORD_SET);
    if (reservedVersionCoordIter != _taskParam.params.end()) {
        node->taskTarget.addTargetDescription(indexlibv2::table::RESERVED_VERSION_COORD_SET,
                                              reservedVersionCoordIter->second);
    }

    for (auto versionId : _taskParam.sourceVersionIds) {
        srcVersions.push_back({sourceRoot, versionId});
    }
    try {
        std::string content = autil::legacy::ToJsonString(srcVersions);
        node->taskTarget.addTargetDescription(config::BS_GENERAL_TASK_SOURCE_VERSIONS, content);
    } catch (const autil::legacy::ExceptionBase& e) {
        assert(false);
    }

    // TODO(hanyao): pass param to plan creator

    node->taskTarget.addTargetDescription("task_description_resolved", "");
}

bool GeneralTaskController::operator==(const TaskController& other) const
{
    const GeneralTaskController* taskController = dynamic_cast<const GeneralTaskController*>(&other);
    if (!taskController) {
        return false;
    }
    return equal(*taskController);
}

bool GeneralTaskController::equal(const GeneralTaskController& other) const
{
    if (_taskName != other._taskName) {
        return false;
    }
    return _partitionTask == other._partitionTask;
}

TaskController* GeneralTaskController::clone()
{
    std::lock_guard<std::mutex> lock(_mutex);
    return new GeneralTaskController(*this);
}

void GeneralTaskController::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("task_config_file_path", _taskConfigFilePath, _taskConfigFilePath);
    json.Jsonize("task_name", _taskName, _taskName);
    json.Jsonize("task_id", _taskId, _taskId);
    json.Jsonize("parallel_num", _parallelNum, _parallelNum);
    json.Jsonize("thread_num", _threadNum, _threadNum);
    _taskParam.jsonizePlan = false;
    json.Jsonize("task_param", _taskParam, _taskParam);
    json.Jsonize("cluster_name", _clusterName, _clusterName);
    json.Jsonize("role_name", _roleName, _roleName);
    bool running = true;
    {
        std::lock_guard<std::mutex> lock(_mutex);
        json.Jsonize("task_info", _taskInfo, _taskInfo);
        running = _taskInfo.empty();
    }

    if (json.GetMode() == autil::legacy::Jsonizable::FROM_JSON && running) {
        if (!initTaskManager(_taskParam, /*plan*/ nullptr)) {
            BS_LOG(ERROR, "init task managers failed");
            throw autil::legacy::ExceptionBase("init task managers failed");
        }
    }
}

void GeneralTaskController::supplementLableInfo(KeyValueMap& info) const
{
    info["task_config"] = _taskConfigFilePath;
    info["config_path"] = _configPath;
    info["task_name"] = _taskName;
    info["task_id"] = _taskId;
    info["role_name"] = _roleName;
    info["parallel_num"] = autil::StringUtil::toString(_parallelNum);
    if (_partitionTask) {
        _partitionTask->supplementLableInfo(info);
    }
}

bool GeneralTaskController::updateConfig()
{
    config::ConfigReaderAccessorPtr readerAccessor;
    _resourceManager->getResource(readerAccessor);
    config::ResourceReaderPtr resourceReader = readerAccessor->getLatestConfig();
    if (!initParallelAndThreadNum(resourceReader, _clusterName, _taskParam.plan.get().taskname(),
                                  _taskParam.plan.get().tasktype())) {
        BS_LOG(ERROR, "get parallel and thread num failed");
        return false;
    }

    _configPath = resourceReader->getOriginalConfigPath();
    return true;
}

std::string GeneralTaskController::getUsingConfigPath() const { return _configPath; }

std::string GeneralTaskController::getTaskInfo()
{
    std::lock_guard<std::mutex> lock(_mutex);
    if (_partitionTask) {
        return _partitionTask->getTaskInfo();
    }
    return _taskInfo;
}

void GeneralTaskController::notifyStopped()
{
    std::lock_guard<std::mutex> lock(_mutex);
    if (_partitionTask) {
        _taskInfo = _partitionTask->getTaskInfo();
    }
}

} // namespace build_service::admin
