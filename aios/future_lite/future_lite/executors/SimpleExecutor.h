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
#ifndef FUTURE_SIMPLE_EXECUTOR_H
#define FUTURE_SIMPLE_EXECUTOR_H

#include <unistd.h>
#include <functional>

#include "future_lite/Executor.h"
#include "future_lite/util/ThreadPool.h"
#include "future_lite/executors/SimpleIOExecutor.h"

#include <thread>
#include <mutex>

namespace future_lite {

namespace executors {

class SimpleExecutor : public Executor {
public:
    using Func = Executor::Func;
    using Context = Executor::Context;

    union ContextUnion {
        Context ctx;
        int64_t id;
    };

public:
    SimpleExecutor(size_t threadNum) : _pool(threadNum) {
        [[maybe_unused]] auto ret = _pool.start();
        assert(ret);
        _ioExecutor.init();
    }
    ~SimpleExecutor() {
        _ioExecutor.destroy();
    }

public:
    bool schedule(Func func) override {
        return _pool.scheduleById(std::move(func)) == util::ThreadPool::ERROR_NONE;
    }
    bool currentThreadInExecutor() const override { return _pool.getCurrentId() != -1; }
    ExecutorStat stat() const override { return ExecutorStat(); }

    size_t currentContextId() const override { return _pool.getCurrentId(); }

    Context checkout() override {
        ContextUnion u;
        // avoid u.id equal to NULLCTX
        u.id = _pool.getCurrentId() | 0x40000000;
        return u.ctx;
    }
    bool checkin(Func func, Context ctx, ScheduleOptions opts) override {
        ContextUnion u;
        u.ctx = ctx;
        if (u.id == -1) {
            return _pool.scheduleById(std::move(func), -1) == util::ThreadPool::ERROR_NONE;
        }
        auto prompt = _pool.getCurrentId() == (u.id & 0xBFFFFFFF) && opts.prompt;
        if (prompt) {
            func();
            return true;
        }
        return _pool.scheduleById(std::move(func), u.id & 0xBFFFFFFF) == util::ThreadPool::ERROR_NONE;
    }
    TimerHandlePtr callLater(Func func, Duration dur) override {
        struct SimpleHandle : TimerHandle {
            bool canceled = false;
            std::mutex mtx;

            void cancel() override {
                std::lock_guard<std::mutex> _(mtx);
                canceled = true;
            }
        };
        auto handle = std::make_shared<SimpleHandle>();
        std::thread([this, handle, func = std::move(func), dur]() {
            std::this_thread::sleep_for(dur);
            std::lock_guard<std::mutex> _(handle->mtx);
            if (!handle->canceled) {
                schedule(std::move(func));
            }
        }).detach();
        return handle;
    }

    void waitAllTaskFinish() {
        _pool.stop();
    }

    IOExecutor* getIOExecutor() override {
        return &_ioExecutor;
    }

private:
    util::ThreadPool _pool;
    SimpleIOExecutor _ioExecutor;
};

} // namespace executors

} // namespace future_lite

#endif
