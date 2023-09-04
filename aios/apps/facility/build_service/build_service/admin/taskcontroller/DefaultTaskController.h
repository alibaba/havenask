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
#ifndef ISEARCH_BS_DEFAULTTASKCONTROLLER_H
#define ISEARCH_BS_DEFAULTTASKCONTROLLER_H

#include "build_service/admin/taskcontroller/TaskController.h"
#include "build_service/common_define.h"
#include "build_service/config/TaskTarget.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

class DefaultTaskController : public TaskController
{
public:
    DefaultTaskController(const std::string& taskId, const std::string& taskName, const TaskResourceManagerPtr& resMgr)
        : TaskController(taskId, taskName, resMgr)
        , _currentTargetIdx(0)
        , _partitionCount(0)
        , _parallelNum(0)
    {
    }
    ~DefaultTaskController();
    DefaultTaskController(const DefaultTaskController&);

private:
    DefaultTaskController& operator=(const DefaultTaskController&);

public:
    TaskController* clone() override;
    bool init(const std::string& clusterName, const std::string& taskConfigPath, const std::string& initParam) override;
    bool operate(TaskController::Nodes& nodes) override;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool updateConfig() override;
    const std::string& getTaskName() const override { return _taskName; }
    virtual bool operator==(const TaskController& other) const override
    {
        const DefaultTaskController* taskController = dynamic_cast<const DefaultTaskController*>(&other);
        if (!taskController) {
            return false;
        }
        return (*this == (*taskController));
    }
    virtual bool operator!=(const TaskController& other) const override { return !(*this == other); }

    void supplementLableInfo(KeyValueMap& info) const override;
    std::string getUsingConfigPath() const override;

protected:
    virtual bool doInit(const std::string& clusterName, const std::string& taskConfigFilePath,
                        const std::string& initParam);
    virtual bool operator==(const DefaultTaskController& other) const;
    virtual bool operator!=(const DefaultTaskController& other) const;

    uint32_t getPartitionCount() const
    {
        if (_currentTargetIdx >= _targets.size()) {
            assert(false);
            return 0;
        }
        return _targets[_currentTargetIdx].getPartitionCount();
    }
    uint32_t getParallelNum() const
    {
        if (_currentTargetIdx >= _targets.size()) {
            assert(false);
            return 0;
        }
        return _targets[_currentTargetIdx].getParallelNum();
    }

protected:
    static const std::string DEFAULT_TARGET_NAME;

protected:
    std::string _clusterName;
    std::string _taskConfigFilePath;
    std::vector<config::TaskTarget> _targets;
    uint32_t _currentTargetIdx;
    uint32_t _partitionCount;
    uint32_t _parallelNum;
    std::string _configPath;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(DefaultTaskController);

}} // namespace build_service::admin

#endif // ISEARCH_BS_DEFAULTTASKCONTROLLER_H
