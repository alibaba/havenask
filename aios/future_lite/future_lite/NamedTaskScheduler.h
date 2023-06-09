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
#include <mutex>
#include <string>
#include <unordered_map>

#include "future_lite/Log.h"
#include "future_lite/TaskScheduler.h"

namespace future_lite {

class NamedTaskScheduler {
public:
    explicit NamedTaskScheduler(future_lite::TaskScheduler *TaskScheduler) : _TaskScheduler(TaskScheduler) {}
    ~NamedTaskScheduler();

public:
    bool StartIntervalTask(const std::string &taskName,
                           std::function<void()> func,
                           future_lite::Executor::Duration duration);
    bool DeleteTask(const std::string &taskName);
    void CleanTasks();
    bool HasTask(const std::string &taskName);
    auto GetTaskScheduler() {
        return _TaskScheduler;
    }

private:
    future_lite::TaskScheduler *_TaskScheduler;
    std::unordered_map<std::string, future_lite::TaskScheduler::Handle> _tasks;
    mutable std::mutex _tasksMutex;

private:
    FL_LOG_DECLARE();
};

} // namespace future_lite
