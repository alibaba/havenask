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

#include <map>
#include <memory>
#include <stdint.h>
#include <string>

#include "autil/legacy/legacy_jsonizable_dec.h"
#include "build_service/admin/AdminTaskBase.h"
#include "build_service/admin/taskcontroller/ProcessorTask.h"
#include "build_service/common/ResourceContainer.h"
#include "build_service/common_define.h"
#include "build_service/proto/ErrorCollector.h"
#include "build_service/proto/WorkerNode.h"

namespace build_service { namespace admin {

class ProcessorTaskWrapper : public AdminTaskBase
{
public:
    ProcessorTaskWrapper(const std::string& clusterName, const ProcessorTaskPtr& processorTask,
                         bool disableOptimize = false);
    ~ProcessorTaskWrapper();

private:
    ProcessorTaskWrapper(const ProcessorTaskWrapper&);
    ProcessorTaskWrapper& operator=(const ProcessorTaskWrapper&);

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool run(proto::WorkerNodes& processorNodes) override;
    void waitSuspended(proto::WorkerNodes& workerNodes) override
    {
        _processorTask->waitSuspended(workerNodes);
        if (_processorTask->isTaskSuspended()) {
            _taskStatus = AdminTaskBase::TASK_SUSPENDED;
        }
    }
    void notifyStopped() override
    {
        AdminTaskBase::notifyStopped();
        _processorTask->notifyStopped();
    }
    const ProcessorTaskPtr& getProcessorTask() const { return _processorTask; }
    void resetProcessorTask(ProcessorTaskPtr& processorTask) { _processorTask = processorTask; }

    bool updateConfig() override;
    bool finish(const KeyValueMap& kvMap) override { return _processorTask->finish(kvMap); }
    std::string getIdentifier() const { return _processorTask->getTaskIdentifier(); }

    bool suspendTask(bool forceSuspend) override
    {
        AdminTaskBase::suspendTask(forceSuspend);
        _processorTask->suspendTask(forceSuspend);
        return true;
    }

    bool resumeTask() override;

    bool isTaskSuspended() const override { return _taskStatus == AdminTaskBase::TASK_SUSPENDED; }

    std::string getClusterName() const { return _clusterName; }
    int64_t getSchemaId() const { return _schemaId; }
    void clearFullWorkerZkNode(const std::string& generationDir) const
    {
        if (_processorTask) {
            _processorTask->clearFullWorkerZkNode(generationDir);
        }
    }

    bool disableOptimize() const { return _disableOptimize; }
    void doSupplementLableInfo(KeyValueMap& info) const override;
    bool getTaskRunningTime(int64_t& intervalInMicroSec) const override;
    std::string getTaskPhaseIdentifier() const override;
    void setBeeperTags(const beeper::EventTagsPtr beeperTags) override;

    void SetProperty(const std::string& key, const std::string& value) override
    {
        _processorTask->SetProperty(key, value);
    }
    bool GetProperty(const std::string& key, std::string& value) const override
    {
        return _processorTask->GetProperty(key, value);
    }
    const std::map<std::string, std::string>& GetPropertyMap() const override
    {
        return _processorTask->GetPropertyMap();
    }
    void SetPropertyMap(const std::map<std::string, std::string>& propertyMap) override
    {
        _processorTask->SetPropertyMap(propertyMap);
    }

private:
    ProcessorTaskPtr createProcessorTask(const std::string& configPath);

private:
    ProcessorTaskPtr _processorTask;
    bool _needReplaceProcessorTask;
    std::string _clusterName;
    int64_t _schemaId;
    bool _disableOptimize;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ProcessorTaskWrapper);

}} // namespace build_service::admin
