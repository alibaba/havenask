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

#include "autil/legacy/jsonizable.h"
#include "build_service/admin/controlflow/TaskResourceManager.h"
#include "build_service/admin/taskcontroller/GeneralNodeGroup.h"
#include "build_service/admin/taskcontroller/OperationTopoManager.h"
#include "build_service/admin/taskcontroller/TaskController.h"
#include "build_service/common_define.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/proto/GeneralTaskInfo.h"
#include "build_service/util/Log.h"
#include "indexlib/file_system/wal/Wal.h"
#include "indexlib/framework/index_task/BasicDefs.h"

namespace build_service::admin {

class SingleGeneralTaskManager
{
public:
    SingleGeneralTaskManager(const std::string& id, const TaskResourceManagerPtr& resourceManager);
    ~SingleGeneralTaskManager() = default;

    SingleGeneralTaskManager& operator=(const SingleGeneralTaskManager&) = delete;

public:
    bool init(const KeyValueMap& initParam, const proto::OperationPlan* plan);
    const std::string& getId() const { return _id; }
    bool operator==(const SingleGeneralTaskManager& other) const;

    bool operate(TaskController::Nodes* nodes, uint32_t parallelNum, uint32_t nodeRunningOpLimit);
    std::string getTaskInfo() const;

    void supplementLableInfo(KeyValueMap& info) const;

private:
    enum class State {
        RUNNING = 0,
        FINISH,
    };

private:
    void prepareNodeGroup(TaskController::Nodes* nodes, std::vector<GeneralNodeGroup>& nodeGroups) const;
    bool recover();
    void recoverNodeTargets(std::vector<GeneralNodeGroup>& nodeGroups);
    void handlePlan(uint32_t parallelNum, uint32_t nodeRunningOpLimit, std::vector<GeneralNodeGroup>& nodeGroups);
    bool storePlan(const proto::OperationPlan& plan);
    bool loadPlan(proto::OperationPlan* plan);

    void finish();
    void reportMetric(int64_t remainOpCount);

private:
    std::string _id;
    TaskResourceManagerPtr _resourceManager;
    State _currentState;
    uint32_t _lastParallelNum;
    std::string _partitionWorkRoot;
    std::string _planPath;
    std::unique_ptr<OperationTopoManager> _topoManager;
    std::unique_ptr<indexlib::file_system::WAL> _wal;
    mutable std::mutex _infoMutex;
    proto::GeneralTaskInfo _taskInfo;
    bool _isRecover;
    std::string _taskType;
    std::string _taskName;

private:
    BS_LOG_DECLARE();
};

} // namespace build_service::admin
