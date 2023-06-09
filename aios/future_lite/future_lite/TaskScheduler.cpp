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
#include "future_lite/TaskScheduler.h"

#include <chrono>
#include <thread>

namespace future_lite {

FL_LOG_SETUP(future_lite, TaskScheduler);

void TaskScheduler::TaskItem::Run() {
    if (_canceled.load(std::memory_order_acquire)) {
        return;
    }

    bool expect = false;
    if (!_running.compare_exchange_strong(expect, true)) {
        return;
    }
    if (_canceled.load(std::memory_order_acquire)) {
        _running.store(false, std::memory_order_release);
        return;
    }

    _func();
    _running.store(false, std::memory_order_release);
}

void TaskScheduler::TaskItem::Join() {
    while (_running.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

TaskScheduler::TaskScheduler(Executor *executor) {
    assert(executor != nullptr);
    _executor = executor;
    _alarm = std::make_unique<util::Alarm>("TaskScheduler");
}

TaskScheduler::~TaskScheduler() {
    CleanTasks();
    _alarm->CancelAll();
}

TaskScheduler::Handle TaskScheduler::ScheduleRepeated(std::function<void()> func, Duration duration) {
    auto taskItem = std::make_shared<TaskItem>(func);
    auto timerFunc = [taskItem]() { taskItem->Run(); };

    auto handle = ScheduleRepeatedInterval(std::move(timerFunc), duration);
    if (handle == INVALID_HANDLE) {
        return INVALID_HANDLE;
    }

    std::lock_guard<std::mutex> lock(_tasksMutex);
    _tasks.emplace(handle, taskItem);
    return handle;
}

void TaskScheduler::CancelTaskInExecutor(Handle handle) {
    size_t waitTimes = 0;
    while (!_alarm->Cancel(handle)) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        FL_LOG(INFO, "Wail [%ld]s for cancel handle [%ld]", ++waitTimes, handle);
    }
}

void TaskScheduler::Cancel(Handle handle) {
    std::shared_ptr<TaskItem> taskItem;
    {
        std::lock_guard<std::mutex> lock(_tasksMutex);
        auto iter = _tasks.find(handle);
        if (iter == _tasks.end()) {
            return;
        }
        taskItem = iter->second;
        _tasks.erase(iter);
    }

    taskItem->SetCanceled();
    CancelTaskInExecutor(handle);
    taskItem->Join();
}

void TaskScheduler::CleanTasks() {
    std::lock_guard<std::mutex> lock(_tasksMutex);
    for (auto &task : _tasks) {
        task.second->SetCanceled();
    }
    for (auto &task : _tasks) {
        CancelTaskInExecutor(task.first);
    }
    for (auto &task : _tasks) {
        task.second->Join();
    }

    _tasks.clear();
}

TaskScheduler::Handle TaskScheduler::ScheduleAfterInterval(Func func, Duration duration) {
    absl::Duration delay = absl::FromChrono(std::chrono::microseconds(duration.count()));
    if (delay <= absl::ZeroDuration()) {
        return Schedule(func);
    }

    auto timerCb = [this, taskFunc = std::move(func)](bool cancel) -> util::Alarm::TimerResult {
        // NOTE: we always execute a delayed task
        // no matter the timer gets cancelled or
        // not.
        Schedule(taskFunc);
        // NOTE: indicate to alarm that this is
        // not a recurring timer.
        return util::Alarm::TimerResult();
    };
    return _alarm->Add(delay, timerCb);
}

TaskScheduler::Handle TaskScheduler::ScheduleRepeatedInterval(Func func, Duration duration) {
    absl::Duration delay = absl::FromChrono(std::chrono::microseconds(duration.count()));

    if (delay <= absl::ZeroDuration()) {
        return Schedule(func);
    }

    auto timerCb = [this, delay, taskFunc = std::move(func)](bool cancel) -> util::Alarm::TimerResult {
        if (cancel) {
            return {/*reschedule=*/false, absl::ZeroDuration()};
        } else {
            // Run user-defined task.
            Schedule(taskFunc);
            // NOTE: indicate to alarm that this is a recurring timer.
            return {/*reschedule=*/true, delay};
        }
    };

    return _alarm->Add(delay, timerCb);
}

} // namespace future_lite