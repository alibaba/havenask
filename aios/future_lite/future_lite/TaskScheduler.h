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
#include <unordered_set>

#include "future_lite/Executor.h"
#include "future_lite/Log.h"
#include "future_lite/util/Alarm.h"

namespace future_lite {

class TaskScheduler {
public:
    using Duration = std::chrono::duration<int64_t, std::micro>;
    using Handle = uint64_t;
    using Func = std::function<void()>;
    static constexpr Handle INVALID_HANDLE = 0;

private:
    class TaskItem {
    public:
        TaskItem(std::function<void()> func) : _func(std::move(func)) {}
        ~TaskItem() = default;
        void Run();
        void Join();
        void SetCanceled() { _canceled.store(true, std::memory_order_release); }

    private:
        std::function<void()> _func;
        std::atomic<bool> _running = false;
        std::atomic<bool> _canceled = false;
    };

public:
    explicit TaskScheduler(Executor *executor);
    ~TaskScheduler();

public:
    Handle ScheduleRepeated(std::function<void()> func, Duration duration);
    void Cancel(Handle taskID);
    Executor *GetExecutor() { return _executor; }

    // Return the number of pending tasks for testing.
    size_t TEST_NumOfPendingTasks() const { return _alarm->TEST_NumOfPendingTimers(); }

private:
    void CleanTasks();

    void CancelTaskInExecutor(Handle handle);

    bool Schedule(Func func) { return _executor->schedule(std::move(func)); }

    // Submit a function to execute asynchronously with 'duration' in the background
    // thread pool.
    Handle ScheduleAfterInterval(Func func, Duration duration);

    // Submit a function to execute repeated asynchronously with 'duration' in the background
    // thread pool.
    Handle ScheduleRepeatedInterval(Func func, Duration duration);

private:
    Executor *_executor;
    // The Timer to schedule task with delay.
    std::unique_ptr<util::Alarm> _alarm;
    std::unordered_map<Handle, std::shared_ptr<TaskItem>> _tasks;
    mutable std::mutex _tasksMutex;

private:
    FL_LOG_DECLARE();
};

} // namespace future_lite
