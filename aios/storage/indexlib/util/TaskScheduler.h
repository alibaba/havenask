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

#include <memory>

#include "autil/Log.h"
#include "indexlib/util/TaskGroup.h"

namespace indexlib { namespace util {

class TaskScheduler;
typedef std::shared_ptr<TaskScheduler> TaskSchedulerPtr;

class TaskScheduler
{
public:
    TaskScheduler() : _taskIdCounter(0), _taskCount(0) {}
    virtual ~TaskScheduler() { CleanTask(); }

public:
    static const int32_t INVALID_TASK_ID = -1;

public:
    bool DeclareTaskGroup(const std::string& taskGroupName, uint64_t intervalInMicroSeconds = 0);
    virtual int32_t AddTask(const std::string& taskGroupName, const TaskItemPtr& taskItem);
    virtual bool DeleteTask(int32_t taskId);
    virtual void CleanTask();

public:
    int32_t GetTaskCount() const { return _taskCount; }
    struct TaskGroupMetric {
        std::string groupName;
        uint64_t tasksExecuteUseTime;
        uint32_t executeTasksCount;
    };

    void GetTaskGroupMetrics(std::vector<TaskGroupMetric>& taskGroupMetrics);

public:
    TaskGroup* TEST_GetTaskGroup(const std::string& groupName) const;

public:
    static TaskSchedulerPtr Create();

private:
    std::map<std::string, size_t> _groupMap;
    std::map<int32_t, size_t> _taskToGroupMap;
    std::vector<TaskGroupPtr> _taskGroups;
    mutable autil::ThreadMutex _lock;

    int32_t _taskIdCounter;
    int32_t _taskCount;

private:
    AUTIL_LOG_DECLARE();
};

}} // namespace indexlib::util
