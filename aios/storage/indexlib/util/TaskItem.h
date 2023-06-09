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

#include <atomic>
#include <functional>
#include <map>
#include <memory>
#include <unistd.h>

#include "indexlib/base/Status.h"

namespace indexlib { namespace util {

template <typename... U>
class TaskItemWithStatus
{
public:
    TaskItemWithStatus() = default;
    virtual ~TaskItemWithStatus() = default;

public:
    virtual Status Run(U... args) = 0;
};
class TaskItem
{
public:
    TaskItem() : _running(false) {}
    virtual ~TaskItem() {}

public:
    virtual void Run() = 0;

public:
    void Join()
    {
        static constexpr size_t kSleepUs = 1000 * 1000;
        while (_running.load(std::memory_order_acquire)) {
            usleep(kSleepUs);
        }
    }
    void SetRunning(bool running) { _running.store(running, std::memory_order_release); }

private:
    std::atomic<bool> _running;
};

class BindFunctionTaskItem : public TaskItem
{
public:
    BindFunctionTaskItem(std::function<void()> func) : _func(func) {}
    ~BindFunctionTaskItem() {}

public:
    void Run() override { _func(); }

private:
    std::function<void()> _func;
};

typedef std::shared_ptr<TaskItem> TaskItemPtr;
}} // namespace indexlib::util
