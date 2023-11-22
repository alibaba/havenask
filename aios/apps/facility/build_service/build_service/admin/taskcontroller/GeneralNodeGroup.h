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

#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "build_service/admin/taskcontroller/TaskController.h"
#include "build_service/common/ResourceContainer.h"
#include "build_service/proto/Admin.pb.h"
#include "build_service/proto/Heartbeat.pb.h"

namespace build_service::admin {

class OperationTopoManager;

class GeneralNodeGroup
{
public:
    GeneralNodeGroup(const std::string& taskType, const std::string& taskName, uint32_t nodeId,
                     OperationTopoManager* topoManager, bool enableTargetBase64Encode,
                     const std::string& partitionWorkRoot);

    uint32_t getNodeId() const { return _nodeId; }
    std::string getTaskType() const { return _taskType; }
    std::string getTaskName() const { return _taskName; }
    size_t getRunningOpCount() const { return _runningOpCount; }

    void addNode(TaskController::Node* node) { _nodes.push_back(node); }
    bool updateTarget();
    void serializeTarget();

    bool collectFinishOp(proto::GeneralTaskWalRecord* walRecord);
    void assignOp(const proto::OperationDescription* op, proto::GeneralTaskWalRecord* walRecord);

    void suspend();
    void resume();
    bool isIdle() const { return _target.ops_size() == 0; }

private:
    const std::string _taskType;
    const std::string _taskName;
    const uint32_t _nodeId; // main node id, not backup node id
    OperationTopoManager* _topoManager;
    const bool _enableTargetBase64Encode;
    const std::string _partitionWorkRoot;

    std::vector<TaskController::Node*> _nodes;
    proto::OperationTarget _target;
    size_t _runningOpCount = 0;

    BS_LOG_DECLARE();
};

} // namespace build_service::admin
