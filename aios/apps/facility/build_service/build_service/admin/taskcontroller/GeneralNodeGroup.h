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

#include <string>
#include <vector>

#include "build_service/admin/taskcontroller/TaskController.h"
#include "build_service/proto/Heartbeat.pb.h"
namespace build_service::admin {

class OperationTopoManager;

class GeneralNodeGroup
{
public:
    GeneralNodeGroup(const std::string& taskType, const std::string& taskName, uint32_t mainNodeId,
                     OperationTopoManager* topoManager)
        : _taskType(taskType)
        , _taskName(taskName)
        , _nodeId(mainNodeId)
        , _topoManager(topoManager)
    {
    }

    void addNode(TaskController::Node* node);
    size_t getMinRunningOpCount() const;
    bool handleFinishedOp(proto::GeneralTaskWalRecord* walRecord);
    void dispatchExecutableOp(const proto::OperationDescription* op, uint32_t nodeRunningOpLimit,
                              proto::GeneralTaskWalRecord* walRecord);
    void serializeTarget() const;
    void updateTarget() const;
    void recoverNodeTarget(const proto::OperationDescription& op);
    bool hasStatus() const;
    void syncPendingOp(uint32_t nodeRunningOpLimit);

private:
    const std::string _taskType;
    const std::string _taskName;
    const uint32_t _nodeId; // main node id, not backup node id
    OperationTopoManager* _topoManager;

    std::vector<TaskController::Node*> _nodes;
    std::vector<proto::OperationTarget> _newTargets;
    std::vector<size_t> _runningOpCounts;

private:
    BS_LOG_DECLARE();
};

} // namespace build_service::admin
