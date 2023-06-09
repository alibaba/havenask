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
#include "indexlib/util/TaskScheduler.h"

using namespace std;
using namespace autil;

using namespace indexlib::util;
namespace indexlib { namespace util {
AUTIL_LOG_SETUP(indexlib.util, TaskScheduler);

bool TaskScheduler::DeclareTaskGroup(const string& taskGroupName, uint64_t intervalInMicroSeconds)
{
    ScopedLock lock(_lock);
    auto it = _groupMap.find(taskGroupName);

    if (it != _groupMap.end()) {
        int32_t groupIdx = it->second;
        if (_taskGroups[groupIdx]->GetTimeInterval() != intervalInMicroSeconds) {
            AUTIL_LOG(ERROR, "declare TaskGroup fail due to "
                             "different time interval setting");
            return false;
        }
        return true;
    }

    _taskGroups.push_back(TaskGroupPtr(new TaskGroup(taskGroupName, intervalInMicroSeconds)));
    _groupMap.insert(make_pair(taskGroupName, _taskGroups.size() - 1));
    return true;
}

int32_t TaskScheduler::AddTask(const string& taskGroupName, const TaskItemPtr& taskItem)
{
    ScopedLock lock(_lock);
    auto it = _groupMap.find(taskGroupName);
    if (it == _groupMap.end()) {
        return INVALID_TASK_ID;
    }
    int32_t groupIdx = it->second;
    int32_t taskId = _taskIdCounter + 1;

    if (!_taskGroups[groupIdx]->AddTask(taskId, taskItem)) {
        return INVALID_TASK_ID;
    }
    _taskToGroupMap[taskId] = groupIdx;
    _taskIdCounter++;
    _taskCount++;
    return taskId;
}

bool TaskScheduler::DeleteTask(int32_t taskId)
{
    std::shared_ptr<TaskGroup> taskGroup = nullptr;
    {
        ScopedLock lock(_lock);
        auto it = _taskToGroupMap.find(taskId);
        if (it == _taskToGroupMap.end()) {
            return false;
        }
        taskGroup = _taskGroups[it->second];
    }
    if (taskGroup->DeleteTask(taskId)) {
        ScopedLock lock(_lock);
        _taskCount--;
        return true;
    }
    return false;
}

void TaskScheduler::CleanTask()
{
    _taskGroups.clear();
    _taskToGroupMap.clear();
    _groupMap.clear();
}

void TaskScheduler::GetTaskGroupMetrics(vector<TaskGroupMetric>& taskGroupMetrics)
{
    map<std::string, size_t>::iterator iter = _groupMap.begin();
    for (; iter != _groupMap.end(); iter++) {
        TaskGroupMetric metric;
        metric.groupName = iter->first;
        metric.tasksExecuteUseTime = _taskGroups[iter->second]->GetAllTaskExecuteTimeInterval();
        metric.executeTasksCount = _taskGroups[iter->second]->GetOneLoopExecuteTaskCount();
        taskGroupMetrics.push_back(metric);
    }
}

TaskSchedulerPtr TaskScheduler::Create() { return TaskSchedulerPtr(new TaskScheduler); }

TaskGroup* TaskScheduler::TEST_GetTaskGroup(const std::string& groupName) const
{
    ScopedLock lock(_lock);
    auto it = _groupMap.find(groupName);
    if (it == _groupMap.end()) {
        return nullptr;
    }
    int32_t groupIdx = it->second;
    return _taskGroups[groupIdx].get();
}
}} // namespace indexlib::util
