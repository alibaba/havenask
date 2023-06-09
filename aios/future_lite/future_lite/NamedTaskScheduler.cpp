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
#include "future_lite/NamedTaskScheduler.h"

namespace future_lite {

FL_LOG_SETUP(future_lite, NamedTaskScheduler);

NamedTaskScheduler::~NamedTaskScheduler() { CleanTasks(); }

bool NamedTaskScheduler::StartIntervalTask(const std::string &taskName,
                                           std::function<void()> func,
                                           future_lite::Executor::Duration duration) {
    std::lock_guard<std::mutex> lock(_tasksMutex);
    auto iter = _tasks.find(taskName);
    if (iter != _tasks.end()) {
        FL_LOG(ERROR, "Add interval task %s failed as already existed.", taskName.c_str());
        return false;
    }

    auto handle = _TaskScheduler->ScheduleRepeated(std::move(func), duration);
    if (handle == future_lite::TaskScheduler::INVALID_HANDLE) {
        FL_LOG(ERROR, "Add interval task %s failed as executor failed.", taskName.c_str());
        return false;
    }

    _tasks.emplace(taskName, handle);

    return true;
}

bool NamedTaskScheduler::DeleteTask(const std::string &taskName) {
    auto handle = future_lite::TaskScheduler::INVALID_HANDLE;
    {
        std::lock_guard<std::mutex> lock(_tasksMutex);
        auto iter = _tasks.find(taskName);
        if (iter == _tasks.end()) {
            FL_LOG(ERROR, "Delete interval task %s failed as not existed.", taskName.c_str());
            return false;
        }

        handle = iter->second;
        _tasks.erase(iter);
    }
    _TaskScheduler->Cancel(handle);

    return true;
}

void NamedTaskScheduler::CleanTasks() {
    std::lock_guard<std::mutex> lock(_tasksMutex);
    for (auto &task : _tasks) {
        _TaskScheduler->Cancel(task.second);
    }

    _tasks.clear();
}

bool NamedTaskScheduler::HasTask(const std::string &taskName) {
    std::lock_guard<std::mutex> lock(_tasksMutex);
    auto iter = _tasks.find(taskName);
    if (iter != _tasks.end()) {
        return true;
    }

    return false;
}

} // namespace future_lite