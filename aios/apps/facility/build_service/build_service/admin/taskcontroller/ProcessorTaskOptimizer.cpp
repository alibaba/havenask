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
#include "build_service/admin/taskcontroller/ProcessorTaskOptimizer.h"

#include "build_service/admin/taskcontroller/ProcessorTaskWrapper.h"
#include "fslib/util/FileUtil.h"
using namespace std;
using namespace build_service::util;
using namespace autil;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, ProcessorTaskOptimizer);

ProcessorTaskOptimizer::ProcessorTaskOptimizer(const TaskResourceManagerPtr& resMgr) : TaskOptimizer(resMgr) {}

ProcessorTaskOptimizer::~ProcessorTaskOptimizer() {}

void ProcessorTaskOptimizer::collect(const AdminTaskBasePtr& taskImpl)
{
    ProcessorTaskWrapperPtr processorTaskWrapper = DYNAMIC_POINTER_CAST(ProcessorTaskWrapper, taskImpl);
    if (!processorTaskWrapper) {
        assert(false);
        return;
    }

    ProcessorTaskPtr processorTask = processorTaskWrapper->getProcessorTask();
    assert(processorTask);
    processorTask->setNeedToRun();

    auto status = processorTaskWrapper->getTaskStatus();
    if (processorTaskWrapper->disableOptimize() || status == AdminTaskBase::TASK_FINISHED ||
        status == AdminTaskBase::TASK_STOPPED) {
        return;
    }

    if (_step == TaskOptimizer::OPTIMIZE) {
        _processorTasks.clear();
        _optimizedMap.clear();
        _originalMap.clear();
        _step = TaskOptimizer::COLLECT;
    }

    string id = processorTask->getTaskIdentifier();
    if (_originalMap.find(id) == _originalMap.end()) {
        _originalMap[id] = processorTask;
    } else {
        // same processor task already exist
        return;
    }

    // suspending task triggered by others not optimize
    auto taskStatus = processorTask->getTaskStatus();
    if ((taskStatus == AdminTaskBase::TASK_SUSPENDING || taskStatus == AdminTaskBase::TASK_SUSPENDED) &&
        processorTask->getSuspendReason() == "for_suspend") {
        return;
    }

    string configPath = fslib::util::FileUtil::normalizeDir(processorTask->getConfigInfo().configPath);
    auto iter = _processorTasks.find(configPath);
    if (iter == _processorTasks.end()) {
        _processorTasks[configPath].push_back(processorTask);
    } else {
        iter->second.push_back(processorTask);
    }
}

AdminTaskBasePtr ProcessorTaskOptimizer::optimize(const AdminTaskBasePtr& taskImpl)
{
    ProcessorTaskWrapperPtr processorTaskWrapper = DYNAMIC_POINTER_CAST(ProcessorTaskWrapper, taskImpl);
    if (!processorTaskWrapper) {
        assert(false);
        return taskImpl;
    }

    if (processorTaskWrapper->disableOptimize() ||
        processorTaskWrapper->getTaskStatus() == AdminTaskBase::TASK_FINISHED) {
        return taskImpl;
    }

    ProcessorTaskPtr processorTask = processorTaskWrapper->getProcessorTask();
    if (_step == TaskOptimizer::COLLECT) {
        auto iter = _processorTasks.begin();
        for (; iter != _processorTasks.end(); iter++) {
            if (!doOptimize(iter->first, iter->second)) {
                _processorTasks.clear();
                _optimizedMap.clear();
                break;
            }
        }
        _step = TaskOptimizer::OPTIMIZE;
    }

    string cluster = processorTaskWrapper->getClusterName();
    int64_t schemaId = processorTaskWrapper->getSchemaId();
    pair<string, int64_t> temp(cluster, schemaId);
    if (_optimizedMap.find(temp) != _optimizedMap.end()) {
        processorTaskWrapper->resetProcessorTask(_optimizedMap[temp]);
        return taskImpl;
    }
    // return first ProcessorTask with the same id when can't be optimized
    string id = processorTask->getTaskIdentifier();
    if (_originalMap.find(id) != _originalMap.end()) {
        processorTaskWrapper->resetProcessorTask(_originalMap[id]);
        return taskImpl;
    }
    return taskImpl;
}

bool ProcessorTaskOptimizer::doOptimize(const std::string& configPath, vector<ProcessorTaskPtr>& tasks)
{
    if (tasks.size() <= 1) {
        return true;
    }

    vector<ProcessorTaskInfo> taskInfos;
    calculateTaskInfos(tasks, taskInfos);
    // select target replace task info
    for (auto& taskInfo : taskInfos) {
        if (taskInfo.taskCount <= 1) {
            // no need optimize
            continue;
        }
        // check all processor task suspended
        bool canReplace = true;
        bool isTablet = false;
        for (auto task : tasks) {
            isTablet = task->isTablet();
            if (!taskInfo.contain(task)) {
                continue;
            }
            task->suspendTask(false, "for_optimize");
            if (!task->isTaskSuspended()) {
                canReplace = false;
            }
        }
        if (canReplace) {
            ProcessorTaskPtr targetTask = createTargetProcessorTask(configPath, taskInfo, isTablet);
            if (!targetTask) {
                return false;
            }
            for (string& cluster : taskInfo.clusterNames) {
                int64_t schemaId;
                if (!targetTask->getSchemaId(cluster, schemaId)) {
                    assert(false);
                    return false;
                }
                pair<string, int64_t> temp(cluster, schemaId);
                _optimizedMap[temp] = targetTask;
            }
        }
    }
    return true;
}

ProcessorTaskPtr ProcessorTaskOptimizer::createTargetProcessorTask(const string& configPath,
                                                                   ProcessorTaskInfo& taskInfo, bool isTablet)
{
    ProcessorTaskPtr task(new ProcessorTask(taskInfo.buildId, taskInfo.step, _resMgr));
    if (!task->init(taskInfo.input, taskInfo.output, taskInfo.clusterNames, taskInfo.configInfo, isTablet)) {
        return ProcessorTaskPtr();
    }
    task->startFromLocator(taskInfo.minLocator);
    string clusterNames = StringUtil::toString(task->getClusterNames(), ",");
    BS_LOG(INFO,
           "optimize processor task, "
           " config path [%s], cluster names[%s]",
           configPath.c_str(), clusterNames.c_str());
    return task;
}

void ProcessorTaskOptimizer::calculateTaskInfos(const vector<ProcessorTaskPtr>& tasks,
                                                vector<ProcessorTaskInfo>& taskInfos)
{
    for (size_t i = 0; i < tasks.size(); i++) {
        bool isInTaskInfos = false;
        for (auto& taskInfo : taskInfos) {
            if (taskInfo.isInRange(tasks[i])) {
                taskInfo.addTask(tasks[i]);
                isInTaskInfos = true;
                break;
            }
        }
        if (!isInTaskInfos) {
            ProcessorTaskInfo taskInfo;
            taskInfo.addTask(tasks[i]);
            taskInfos.push_back(taskInfo);
        }
    }
    for (auto& taskInfo : taskInfos) {
        sort(taskInfo.clusterNames.begin(), taskInfo.clusterNames.end());
    }
}

bool ProcessorTaskOptimizer::ProcessorTaskInfo::contain(const ProcessorTaskPtr& processorTask)
{
    if (!isInRange(processorTask)) {
        return false;
    }
    auto& processorClusterNames = processorTask->getClusterNames();
    for (auto& clusterName : processorClusterNames) {
        if (find(clusterNames.begin(), clusterNames.end(), clusterName) == clusterNames.end()) {
            return false;
        }
    }
    return true;
}

bool ProcessorTaskOptimizer::ProcessorTaskInfo::isEqual(const ProcessorTaskPtr& processorTask)
{
    if (!(input == processorTask->getInput())) {
        return false;
    }
    if (step != processorTask->getBuildStep()) {
        return false;
    }
    if (clusterNames != processorTask->getClusterNames()) {
        return false;
    }
    common::Locator locator = processorTask->getLocator();
    if (locator == minLocator && locator == maxLocator) {
        return true;
    }
    return false;
}

bool ProcessorTaskOptimizer::ProcessorTaskInfo::isInRange(const ProcessorTaskPtr& processorTask)
{
    if (!(input == processorTask->getInput())) {
        return false;
    }
    if (step != processorTask->getBuildStep()) {
        return false;
    }
    auto tmpConfigInfo = processorTask->getConfigInfo();
    if (tmpConfigInfo.configName != configInfo.configName || tmpConfigInfo.preStage != configInfo.preStage ||
        tmpConfigInfo.stage != configInfo.stage) {
        return false;
    }
    common::Locator locator = processorTask->getLocator();
    if (!locator.IsSameSrc(minLocator, true)) {
        return false;
    }
    common::Locator smallestLocator = locator.GetOffset() >= minLocator.GetOffset() ? minLocator : locator;
    common::Locator largestLocator = locator.GetOffset() > maxLocator.GetOffset() ? locator : maxLocator;
    if (largestLocator.GetOffset().first - smallestLocator.GetOffset().first < 180000000) { // 3 min
        return true;
    }
    return false;
}

void ProcessorTaskOptimizer::ProcessorTaskInfo::addTask(const ProcessorTaskPtr& processorTask)
{
    taskCount++;
    buildId = processorTask->getBuildId();
    input = processorTask->getInput();
    output.combine(processorTask->getOutput());
    configInfo = processorTask->getConfigInfo();
    step = processorTask->getBuildStep();
    common::Locator locator = processorTask->getLocator();
    if (clusterNames.size() == 0) {
        minLocator = locator;
        maxLocator = locator;
        clusterNames = processorTask->getClusterNames();
        return;
    }
    minLocator = locator.GetOffset() >= minLocator.GetOffset() ? minLocator : locator;
    maxLocator = locator.GetOffset() > maxLocator.GetOffset() ? locator : maxLocator;

    auto clusters = processorTask->getClusterNames();
    for (auto clusterName : clusters) {
        if (find(clusterNames.begin(), clusterNames.end(), clusterName) == clusterNames.end()) {
            clusterNames.push_back(clusterName);
        }
    }
}

}} // namespace build_service::admin
