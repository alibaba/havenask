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
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "build_service/admin/AdminTaskBase.h"
#include "build_service/admin/controlflow/TaskResourceManager.h"
#include "build_service/admin/taskcontroller/ProcessorInput.h"
#include "build_service/admin/taskcontroller/ProcessorParamParser.h"
#include "build_service/admin/taskcontroller/ProcessorTask.h"
#include "build_service/admin/taskcontroller/TaskOptimizer.h"
#include "build_service/common/Locator.h"
#include "build_service/common/ProcessorOutput.h"
#include "build_service/common/ResourceContainer.h"
#include "build_service/common_define.h"
#include "build_service/proto/BasicDefs.pb.h"

namespace build_service { namespace admin {

class ProcessorTaskOptimizer : public TaskOptimizer
{
public:
    ProcessorTaskOptimizer(const TaskResourceManagerPtr& resMgr);
    ~ProcessorTaskOptimizer();

private:
    ProcessorTaskOptimizer(const ProcessorTaskOptimizer&);
    ProcessorTaskOptimizer& operator=(const ProcessorTaskOptimizer&);

public:
    void collect(const AdminTaskBasePtr& taskImpl) override;
    AdminTaskBasePtr optimize(const AdminTaskBasePtr& taskImpl) override;

private:
    struct ProcessorTaskInfo {
        ProcessorTaskInfo() : taskCount(0) {}
        std::vector<std::string> clusterNames;
        int32_t taskCount;
        proto::BuildId buildId;
        proto::BuildStep step;
        ProcessorInput input;
        common::ProcessorOutput output;
        ProcessorConfigInfo configInfo;
        common::Locator minLocator;
        common::Locator maxLocator;
        bool isInRange(const ProcessorTaskPtr& processorTask);
        void addTask(const ProcessorTaskPtr& processorTask);
        bool isEqual(const ProcessorTaskPtr& processorTask); // useless, to delete
        bool contain(const ProcessorTaskPtr& processorTask);
    };

private:
    bool doOptimize(const std::string& configPath, std::vector<ProcessorTaskPtr>& tasks);
    void calculateTaskInfos(const std::vector<ProcessorTaskPtr>& tasks, std::vector<ProcessorTaskInfo>& taskInfos);
    ProcessorTaskPtr createTargetProcessorTask(const std::string& configPath, ProcessorTaskInfo& taskInfo,
                                               bool isTablet);

private:
    std::map<std::string, std::vector<ProcessorTaskPtr>> _processorTasks;      // to optimize processor tasks
    std::map<std::pair<std::string, int64_t>, ProcessorTaskPtr> _optimizedMap; // optimized processor tasks
    std::map<std::string, ProcessorTaskPtr> _originalMap; // original processor tasks, use when unoptimize || recover

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ProcessorTaskOptimizer);

}} // namespace build_service::admin
