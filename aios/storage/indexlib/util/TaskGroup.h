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

#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/Thread.h"
#include "indexlib/util/TaskItem.h"

namespace indexlib { namespace util {

class TaskGroup
{
public:
    TaskGroup(const std::string& groupName, uint64_t intervalInMicroSeconds)
        : _groupName(groupName)
        , _timeInterval(intervalInMicroSeconds)
        , _running(false)
        , _allTaskExecutedTimeInterval(0)
        , _oneLoopExecuteTaskCount(0)
    {
    }

    ~TaskGroup()
    {
        _running = false;
        _backGroundThreadPtr.reset();
        _idToTaskMap.clear();
        _taskItems.clear();
    }

public:
    bool AddTask(int32_t taskId, const TaskItemPtr& taskItem);

    bool DeleteTask(int32_t taskId);

    uint64_t GetTimeInterval() const { return _timeInterval; }
    uint64_t GetAllTaskExecuteTimeInterval() const
    {
        return _allTaskExecutedTimeInterval.load(std::memory_order_acquire);
    }
    uint64_t GetOneLoopExecuteTaskCount() const { return _oneLoopExecuteTaskCount.load(std::memory_order_acquire); }

public:
    TaskItemPtr TEST_GetTaskItem(int32_t taskId) const;

private:
    void BackGroundThread();

private:
    std::string _groupName;
    uint64_t _timeInterval;
    mutable autil::ThreadMutex _lock;
    std::map<int32_t, size_t> _idToTaskMap;
    std::vector<TaskItemPtr> _taskItems;
    autil::ThreadPtr _backGroundThreadPtr;
    bool _running;
    std::atomic<uint64_t> _allTaskExecutedTimeInterval;
    std::atomic<uint32_t> _oneLoopExecuteTaskCount;

private:
    AUTIL_LOG_DECLARE();
    friend class TaskGroupTest;
};

typedef std::shared_ptr<TaskGroup> TaskGroupPtr;
}} // namespace indexlib::util
