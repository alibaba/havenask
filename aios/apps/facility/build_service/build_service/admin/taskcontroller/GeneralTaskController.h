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

#include "build_service/admin/taskcontroller/SingleGeneralTaskManager.h"
#include "build_service/admin/taskcontroller/TaskController.h"
#include "build_service/common_define.h"
#include "build_service/config/TaskTarget.h"
#include "build_service/proto/JsonizableProtobuf.h"
#include "build_service/util/Log.h"
#include "indexlib/base/Constant.h"

namespace build_service::admin {

struct GeneralTaskParam final : public autil::legacy::Jsonizable {
    GeneralTaskParam() = default;

    std::string taskEpochId;
    std::string partitionIndexRoot;
    uint64_t branchId = 0;
    uint32_t generationId = 0;
    std::vector<indexlibv2::versionid_t> sourceVersionIds;
    std::map<std::string, std::string> params;

    bool jsonizePlan = true;
    proto::JsonizableProtobuf<proto::OperationPlan> plan;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        json.Jsonize("task_epoch_id", taskEpochId);
        json.Jsonize("partition_index_root", partitionIndexRoot);
        json.Jsonize("generation_id", generationId);
        json.Jsonize("branch_id", branchId, branchId);
        json.Jsonize("source_version_ids", sourceVersionIds);
        json.Jsonize("params", params);

        if (jsonizePlan) {
            // do not jsonize plan to zookeeper, too heavy
            json.Jsonize("plan", plan);
        }
    }
};

class GeneralTaskController : public TaskController
{
public:
    GeneralTaskController(const std::string& taskId, const std::string& taskName, const TaskResourceManagerPtr& resMgr)
        : TaskController(taskId, taskName, resMgr)
    {
    }
    ~GeneralTaskController() = default;

private:
    GeneralTaskController(const GeneralTaskController&);
    GeneralTaskController& operator=(const GeneralTaskController&) = delete;

public:
    TaskController* clone() override;
    bool init(const std::string& clusterName, const std::string& taskConfigPath, const std::string& initParam) override;
    bool operate(TaskController::Nodes& nodes) override;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool updateConfig() override;
    const std::string& getTaskName() const override { return _taskName; }
    bool operator==(const TaskController& other) const override;
    bool operator!=(const TaskController& other) const override { return !(*this == other); }
    void supplementLableInfo(KeyValueMap& info) const override;
    std::string getUsingConfigPath() const override;
    std::string getTaskInfo() override;
    void notifyStopped() override;
    bool isSupportBackup() const override { return true; }

private:
    bool equal(const GeneralTaskController& other) const;
    bool initTaskManager(const GeneralTaskParam& param, const proto::OperationPlan* plan);
    void addDescription(const GeneralTaskParam& param, TaskController::Node* node);
    bool initParallelAndThreadNum(const config::ResourceReaderPtr& resourceReader, const std::string& clusterName,
                                  const std::string& planTaskName, const std::string& planTaskType);
    std::string getRoleName(const std::string& taskName, const std::string& taskType);

private:
    std::string _clusterName;
    std::string _taskConfigFilePath;
    std::string _configPath;
    uint32_t _parallelNum = 1;
    uint32_t _threadNum = 2;
    GeneralTaskParam _taskParam;

    std::mutex _mutex;
    std::string _taskInfo;
    std::string _roleName;
    std::shared_ptr<SingleGeneralTaskManager> _partitionTask;

private:
    BS_LOG_DECLARE();
};

} // namespace build_service::admin
