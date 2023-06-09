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
#ifndef FUTURE_LITE_EXECUTOR_H
#define FUTURE_LITE_EXECUTOR_H


#include <functional>
#include "future_lite/Config.h"
#include "future_lite/Common.h"
#include "future_lite/IOExecutor.h"
#include "future_lite/util/Condition.h"
#include <string>
#include <chrono>
#include <thread>


#if FUTURE_LITE_COROUTINE_AVAILABLE
#include "future_lite/experimental/coroutine.h"
#endif

namespace future_lite {

struct ExecutorStat {
    size_t pendingTaskCount = 0;
    ExecutorStat() = default;
};
struct ScheduleOptions {
    bool prompt = true;
    ScheduleOptions() = default;
};

class TimerHandle {
public:
    virtual ~TimerHandle() = default;
    virtual void cancel() = 0;
};

struct CurrentExecutor {};

#if FUTURE_LITE_COROUTINE_AVAILABLE
constexpr CurrentExecutor GetCurrentExecutor() { return {}; }
#endif

class Executor {
public:
    using Context = void *;
    static constexpr Context NULLCTX = nullptr;
    using Func = std::function<void()>;
    using Duration = std::chrono::duration<int64_t, std::micro>;
    using TimerHandlePtr = std::shared_ptr<TimerHandle>;

    Executor(std::string name = "default") : _name(std::move(name)){};
    virtual ~Executor() {};

    Executor(const Executor &) = delete;
    Executor& operator = (const Executor &) = delete;

public:
    virtual bool schedule(Func func) = 0;
    // Scheduling cancelable delayed callbacks, TimerHandlePtr's destroy should not cancel task
    virtual TimerHandlePtr callLater(Func func, Duration dur) { return {}; };
    virtual bool currentThreadInExecutor() const = 0;
    virtual ExecutorStat stat() const = 0;
    virtual size_t currentContextId() const { return 0; };
    virtual Context checkout() { return NULLCTX; }
    virtual bool checkin(Func func, Context ctx, ScheduleOptions opts) {
        return schedule(std::move(func));
    }
    virtual bool checkin(Func func, Context ctx) {
        static ScheduleOptions opts;
        return checkin(std::move(func), ctx, opts);
    }
    const std::string &name() const { return _name; }
    virtual IOExecutor* getIOExecutor() = 0;

#if FUTURE_LITE_COROUTINE_AVAILABLE
    class TimeAwaitable;
    class TimeAwaiter;
    TimeAwaitable after(Duration dur);
#endif

private:
    std::string _name;
};

#if FUTURE_LITE_COROUTINE_AVAILABLE

class Executor::TimeAwaiter {
public:
    TimeAwaiter(Executor *ex, Executor::Duration dur) : _ex(ex), _dur(dur) {}

public:
    bool await_ready() const noexcept {
        return false;
    }

    template <typename PromiseType>
    void await_suspend(
        std::coroutine_handle<PromiseType> continuation)
    {
        std::function<void()> func = [c = continuation]() mutable {
            c.resume();
        };
        [[maybe_unused]] auto handle = _ex->callLater(func, _dur);
    }
    void await_resume() const noexcept {}

private:
    Executor *_ex;
    Executor::Duration _dur;
};

class Executor::TimeAwaitable {
public:
    TimeAwaitable(Executor *ex, Executor::Duration dur) : _ex(ex), _dur(dur) {}

    auto coAwait(Executor *) { return Executor::TimeAwaiter(_ex, _dur); }

private:
    Executor *_ex;
    Executor::Duration _dur;
};

Executor::TimeAwaitable
inline Executor::after(Executor::Duration dur) {
    return Executor::TimeAwaitable(this, dur);
};

#endif

}  // namespace future_lite

#include "future_lite/ExecutorCreator.h"

/*
  添加一种新的 Executor 实现，需要完成以下2个步骤:
  1.在你的 Executor cpp 内调用 REGISTER_FUTURE_LITE_EXECUTOR(type) 的宏，
  例如， REGISTER_FUTURE_LITE_EXECUTOR(simple) { return new SimpleExecutor(params); }
  params 是 future_lite::ExecutorCreator::Parameters 结构，用来传递参数
  2.在该文件对应的目标BUILD目标上添加 alwayslink = True, 可参考 SimpleExecutor 和对应的BUILD文件

  如果想确保某种类型的 Executor 一定被注册，可以使用 CHECK_FUTURE_LITE_EXECUTOR 来检测，如果未链接会编译失败
  这两个宏都需要在 global namesapce 使用

*/

#define REGISTER_FUTURE_LITE_EXECUTOR(TYPE)                                                                            \
    class Executor##TYPE##Creator : public ::future_lite::detail::ExecutorCreatorBase {                                \
    public:                                                                                                            \
        Executor##TYPE##Creator() = default;                                                                           \
        ~Executor##TYPE##Creator() {}                                                                                  \
        std::unique_ptr<future_lite::Executor> Create(const Parameters &param) override;                               \
    };                                                                                                                 \
    bool Executor##TYPE##Creator_dummy = false;                                                                        \
    __attribute__((constructor(102))) void Register##Executor##TYPE##Creator() {                                       \
        ::future_lite::detail::ExecutorCreatorBase::Register(#TYPE, std::make_unique<Executor##TYPE##Creator>());      \
    }                                                                                                                  \
    std::unique_ptr<future_lite::Executor> Executor##TYPE##Creator::Create(const Parameters &params)

#define CHECK_FUTURE_LITE_EXECUTOR_OLD(TYPE)                                                                           \
    extern bool Executor##TYPE##Creator_dummy;                                                                         \
    namespace {                                                                                                        \
    __attribute__((constructor(103))) void Check##TYPE##Executor() {                                                   \
        ::Executor##TYPE##Creator_dummy = false;                                                                       \
        if (!future_lite::ExecutorCreator::HasExecutor(#TYPE)) {                                                       \
            std::fprintf(stdout, "executor " #TYPE " not registed, LINK it to your bazel target");                     \
            ::fflush(stdout);                                                                                          \
            std::abort();                                                                                              \
        }                                                                                                              \
    }                                                                                                                  \
    }

#define CHECK_FUTURE_LITE_EXECUTOR(TYPE)                                                                               \
    extern bool Executor##TYPE##Creator_dummy;                                                                         \
    namespace {                                                                                                        \
    __attribute__((constructor(103))) void Check##TYPE##Executor() { ::Executor##TYPE##Creator_dummy = false; }        \
    }

#endif
