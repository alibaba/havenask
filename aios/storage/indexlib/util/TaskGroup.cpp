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
#include "indexlib/util/TaskGroup.h"

#include <algorithm>
#include <ext/alloc_traits.h>
#include <functional>
#include <iosfwd>
#include <unistd.h>
#include <utility>

#include "autil/TimeUtility.h"

using namespace std;
using namespace autil;

using namespace indexlib::util;
namespace indexlib { namespace util {
AUTIL_LOG_SETUP(indexlib.util, TaskGroup);

bool TaskGroup::AddTask(int32_t taskId, const TaskItemPtr& taskItem)
{
    ScopedLock lock(_lock);
    if (!_backGroundThreadPtr) {
        _running = true;
        _backGroundThreadPtr = autil::Thread::createThread(std::bind(&TaskGroup::BackGroundThread, this), "indexTask");
    }

    if (!_backGroundThreadPtr) {
        AUTIL_LOG(ERROR, "create thread fail for taskGroup[%s]", _groupName.c_str());
        return false;
    }

    if (_idToTaskMap.find(taskId) != _idToTaskMap.end()) {
        AUTIL_LOG(ERROR, "add task to TaskGroup[%s] failed due to duplicate taskID", _groupName.c_str());
        return false;
    }

    size_t idx = 0;
    for (; idx < _taskItems.size(); idx++) {
        if (!_taskItems[idx]) {
            break;
        }
    }

    if (idx == _taskItems.size()) {
        _taskItems.push_back(taskItem);
    } else {
        _taskItems[idx] = taskItem;
    }

    _idToTaskMap[taskId] = idx;
    return true;
}

void TaskGroup::BackGroundThread()
{
    size_t currentIdx = 0;
    int64_t beginTime = 0;
    uint32_t executeTaskCount = 0;
    size_t taskSize = 0;
    TaskItemPtr task;
    while (_running) {
        if (currentIdx == 0) {
            usleep(_timeInterval);
            beginTime = TimeUtility::currentTimeInMicroSeconds();
            executeTaskCount = 0;
        }
        {
            ScopedLock lock(_lock);
            task = _taskItems[currentIdx];
            taskSize = _taskItems.size();
            if (task) {
                task->SetRunning(true);
            }
        }
        if (task) {
            task->Run();
            task->SetRunning(false);
            task.reset();
            executeTaskCount++;
        }
        if (currentIdx + 1 == taskSize) {
            uint64_t interval = TimeUtility::currentTimeInMicroSeconds() - beginTime;
            _allTaskExecutedTimeInterval.store(interval, std::memory_order_release);
            _oneLoopExecuteTaskCount.store(executeTaskCount, std::memory_order_release);
            if (interval >= 10000000) // interval more than 10 second
            {
                AUTIL_LOG(WARN,
                          "TaskGroup[%s] execute [%d] tasks use time [%ld us]"
                          " more than 10 seconds",
                          _groupName.c_str(), executeTaskCount, interval);
            }
        }
        currentIdx = (currentIdx + 1) % taskSize;
    }
}

bool TaskGroup::DeleteTask(int32_t taskId)
{
    TaskItemPtr task;
    {
        ScopedLock lock(_lock);
        auto it = _idToTaskMap.find(taskId);
        if (it == _idToTaskMap.end()) {
            return false;
        }

        size_t taskIdx = it->second;
        task = std::move(_taskItems[taskIdx]);
        _idToTaskMap.erase(it);
    }
    if (task) {
        task->Join();
    }
    return true;
}

TaskItemPtr TaskGroup::TEST_GetTaskItem(int32_t taskId) const
{
    ScopedLock lock(_lock);
    auto it = _idToTaskMap.find(taskId);
    if (it == _idToTaskMap.end()) {
        return TaskItemPtr();
    }

    return _taskItems[it->second];
}
}} // namespace indexlib::util
