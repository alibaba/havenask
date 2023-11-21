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
#include "build_service/admin/taskcontroller/ExtractDocTaskController.h"

#include <assert.h>
#include <ext/alloc_traits.h>
#include <iosfwd>
#include <memory>
#include <stdint.h>

#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/common/CheckpointAccessor.h"
#include "build_service/common/IndexCheckpointFormatter.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/config/TaskTarget.h"
#include "build_service/proto/Admin.pb.h"

using namespace std;
using build_service::common::CheckpointAccessorPtr;
using build_service::common::IndexCheckpointFormatter;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, ExtractDocTaskController);

ExtractDocTaskController::~ExtractDocTaskController() {}

bool ExtractDocTaskController::init(const std::string& clusterName, const std::string& taskConfigPath,
                                    const std::string& initParam)
{
    CheckpointAccessorPtr checkPointAccessor;
    _resourceManager->getResource(checkPointAccessor);
    string checkpointId = IndexCheckpointFormatter::getIndexCheckpointId(true, clusterName);
    string checkPointName;
    string checkPointValue;
    if (!checkPointAccessor->getLatestCheckpoint(checkpointId, checkPointName, checkPointValue)) {
        BS_LOG(ERROR, "get checkpoint failed for task [%s], cluster is [%s]", _taskId.c_str(), clusterName.c_str());
        return false;
    }
    proto::CheckpointInfo checkpoint;
    IndexCheckpointFormatter::decodeCheckpoint(checkPointValue, checkpoint);
    _processorCheckpoint = autil::StringUtil::toString(checkpoint.processorcheckpoint());
    string errorMsg;
    if (!checkPointAccessor->createSavepoint(_taskId, checkpointId, checkPointName, errorMsg)) {
        BS_LOG(ERROR, "create savepoint failed for task [%s], error is [%s]", _taskId.c_str(), errorMsg.c_str());
        return false;
    }

    if (!DefaultTaskController::init(clusterName, taskConfigPath, initParam)) {
        return false;
    }

    _checkpoints.clear();
    _checkpoints.resize(_partitionCount * _parallelNum);
    _targets.clear();
    config::TaskTarget doTaskTarget("do_task");
    doTaskTarget.addTargetDescription(config::BS_SNAPSHOT_VERSION, checkPointName);
    doTaskTarget.addTaskConfigPath(taskConfigPath);
    doTaskTarget.setPartitionCount(_partitionCount);
    doTaskTarget.setParallelNum(_parallelNum);

    auto taskParam = initParam;
    if (taskParam.empty()) {
        BS_LOG(ERROR, "extract doc taskParam is empty");
        return false;
    }
    doTaskTarget.addTargetDescription("task_param", taskParam);
    _targets.push_back(doTaskTarget);
    return true;
}

bool ExtractDocTaskController::updateConfig()
{
    BS_LOG(INFO, "extract doc task update config will do nothing");
    return true;
}

void ExtractDocTaskController::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    DefaultTaskController::Jsonize(json);
    json.Jsonize("checkpoints", _checkpoints, _checkpoints);
    json.Jsonize("processor_checkpoint", _processorCheckpoint, _processorCheckpoint);
}

bool ExtractDocTaskController::operate(TaskController::Nodes& taskNodes)
{
    if (taskNodes.size() == 0) {
        uint32_t nodeSize = getPartitionCount() * getParallelNum();
        assert(nodeSize == _checkpoints.size());
        taskNodes.reserve(nodeSize);
        for (uint32_t i = 0; i < nodeSize; ++i) {
            TaskController::Node node;
            node.nodeId = i;
            node.instanceIdx = i;
            auto& target = _targets[_currentTargetIdx];
            target.updateTargetDescription(config::BS_EXTRACT_DOC_CHECKPOINT, _checkpoints[i]);
            node.taskTarget = _targets[_currentTargetIdx];
            taskNodes.push_back(node);
        }
        return false;
    }

    bool isFinished = true;
    for (uint32_t i = 0; i < taskNodes.size(); ++i) {
        if (taskNodes[i].reachedTarget) {
            continue;
        }
        isFinished = false;
        if (!taskNodes[i].statusDescription.empty()) {
            config::TaskTarget current;
            FromJsonString(current, taskNodes[i].statusDescription);
            string value;
            if (current.getTargetDescription(config::BS_EXTRACT_DOC_CHECKPOINT, value)) {
                _checkpoints[i] = value;
            }
        }
        auto target = _targets[_currentTargetIdx];
        target.updateTargetDescription(config::BS_EXTRACT_DOC_CHECKPOINT, _checkpoints[i]);
        taskNodes[i].taskTarget = target;
    }
    if (!isFinished) {
        return false;
    }
    taskNodes.clear();
    _currentTargetIdx++;

    if (_currentTargetIdx == _targets.size()) {
        _currentTargetIdx = 0;
        return true;
    }
    return false;
}

}} // namespace build_service::admin
