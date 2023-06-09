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
#ifndef FUTURE_LITE_CORO_TASK_MANAGER_H
#define FUTURE_LITE_CORO_TASK_MANAGER_H

#include "future_lite/Common.h"
#include "future_lite/Invoke.h"
#include "future_lite/experimental/coroutine.h"
#include "future_lite/Executor.h"
#include "future_lite/coro/Mutex.h"
#include "future_lite/coro/ConditionVariable.h"
#include "future_lite/coro/Sleep.h"
#include "future_lite/coro/Traits.h"
#include <functional>
#include <atomic>
#include <mutex>
#include <unordered_map>

namespace future_lite {
namespace coro {

class TaskManager {
private:
    struct TaskCondition {
        std::atomic<bool> running;
        coro::Mutex mtx;
        TaskCondition() : running(false) {}
    };
    using TaskConditionPtr = std::shared_ptr<TaskCondition>;

public:
    using TaskFunc = std::function<void()>;

public:
    TaskManager(Executor *ex) : _executor(ex), _activeRepeatedTask(0) {}

public:
    TaskManager(const TaskManager &) = delete;
    TaskManager &operator=(const TaskManager &) = delete;

    TaskManager(TaskManager &&other) {}
    TaskManager &operator=(TaskManager &&other) {
        if (this != &other) {
        }
        return *this;
    }

public:
    bool addTask(TaskFunc func) {
        assert(_executor);
        return _executor->schedule(std::move(func));
    }

    template <typename F, typename Rep, typename Period>
    Lazy<bool> addRepeatedTask(const std::string &taskName, F &&func,
                               std::chrono::duration<Rep, Period> interval);
    Lazy<bool> deleteRepeatedTask(const std::string &taskName, bool waitFinish = true);

    size_t getActiveRepeatedTask() { return _activeRepeatedTask.load(); }

private:
    Executor *_executor;
    std::unordered_map<std::string, TaskConditionPtr> _taskMap;
    coro::Mutex _mtx;
    std::atomic<size_t> _activeRepeatedTask;
};

/////////////////////////////////////////////////////////////////////////////////

template <typename F, typename Rep, typename Period>
inline Lazy<bool> TaskManager::addRepeatedTask(const std::string &taskName, F &&func,
                                               std::chrono::duration<Rep, Period> interval) {
    auto lock = co_await _mtx.coScopedLock();
    auto iter = _taskMap.find(taskName);
    if (iter != _taskMap.end()) {
        co_return false;
    }
    _activeRepeatedTask++;
    auto taskCondition = std::make_shared<TaskCondition>();
    taskCondition->running.store(true, std::memory_order_release);
    auto lambda = [](std::decay_t<F> func, TaskConditionPtr taskCondition,
                     std::chrono::duration<Rep, Period> interval,
                     std::atomic<size_t> &activeRepeatedTask) -> Lazy<> {    
        while (taskCondition->running.load()) {
            {
                co_await taskCondition->mtx.coScopedLock();
                if (!taskCondition->running.load()) {
                    break;
                }
                using R = typename std::invoke_result_t<std::decay_t<F>>;
                if constexpr (detail::HasCoAwaitMethod<R>::value ||
                              detail::HasGlobalCoAwaitOperator<R>::value ||
                              detail::HasMemberCoAwaitOperator<R>::value) {
                    co_await func();
                } else {
                    func();
                }
            }
            co_await sleep(interval);
        }
        activeRepeatedTask--;
    };
    _taskMap[taskName] = taskCondition;
    lambda(std::forward<F>(func), taskCondition, interval, _activeRepeatedTask)
        .via(_executor)
        .start([](Try<void> &&t) {});

    co_return true;
}

inline Lazy<bool> TaskManager::deleteRepeatedTask(const std::string &taskName,
                                                  bool waitFinish) {
    [[maybe_unused]]auto lock = _mtx.coScopedLock();
    auto iter = _taskMap.find(taskName);
    if (iter == _taskMap.end()) {
        co_return false;
    }
    if (waitFinish) {
        auto taskLock = co_await iter->second->mtx.coScopedLock();
        iter->second->running.store(false, std::memory_order_relaxed);
    } else {
        iter->second->running.store(false, std::memory_order_relaxed);
    }
    _taskMap.erase(iter);
    co_return true;
}
}  // namespace coro
}  // namespace future_lite

#endif
