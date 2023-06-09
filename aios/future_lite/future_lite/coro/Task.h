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
#ifndef FUTURE_LITE_CORO_TASK_H
#define FUTURE_LITE_CORO_TASK_H

#include "future_lite/Common.h"
#include "future_lite/Invoke.h"
#include "future_lite/coro/Util.h"
#include "future_lite/util/Condition.h"
#include "future_lite/experimental/coroutine.h"
#include <exception>

#include <mutex>
#include <atomic>

namespace future_lite {
namespace coro {
namespace detail {

template <typename T>
struct TaskPromise;

}

template <typename T>
class [[nodiscard]] Task  {
public:
    using promise_type = detail::TaskPromise<T>;
    using Handle = std::coroutine_handle<promise_type>;
    struct Awaiter {
        constexpr bool await_ready() const noexcept { return true; }
        void await_suspend(std::coroutine_handle<> continuation) const noexcept {
        }
        FL_INLINE T await_resume() noexcept { return std::move(task->stealValue()); }

        Task* task;

        Awaiter(Task* t) : task(t) {}
    };

public:
    Task(Handle coro) : _coro(coro), _hasValue(false) { coro.promise().task = this; }
    ~Task() {
        if (FL_UNLIKELY(_hasValue)) {
            _value.~T();
        }
    }
    Task(Task && other)
        : _coro(std::exchange(other._coro, nullptr)),
          _hasValue(std::exchange(other._hasValue, false)) {
        new (std::addressof(_value)) T(std::move(other._value));
        _coro.promise().task = this;
    }

    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;
    Task& operator=(Task&&) = delete;

public:
    auto coAwait(Executor * ex) {
        return Awaiter(this);
    }
    auto operator co_await() { return Awaiter(this); }

private:
    template <typename C>
    FL_INLINE void setValue(C && v) noexcept {
        new (std::addressof(_value)) T(std::forward<C>(v));
        _hasValue = true;
    }

    FL_INLINE T&& stealValue() noexcept {
        _hasValue = false;
        return std::move(_value);
    }

private:
    Handle _coro;
    union {
        T _value;
    };
    bool _hasValue;

private:
    friend struct detail::TaskPromise<T>;
};

template <>
class [[nodiscard]] Task<void> {
public:
    using promise_type = detail::TaskPromise<void>;
    using Handle = std::coroutine_handle<promise_type>;
    struct Awaiter {
        constexpr bool await_ready() const noexcept { return true; }
        void await_suspend(std::coroutine_handle<> continuation) const noexcept {
        }
        FL_INLINE void await_resume() const noexcept {}
        Awaiter() = default;
    };

public:
    Task() = default;
    Task(Task &&) = default;

    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;
    Task& operator=(Task&&) = delete;
public:
    auto coAwait(Executor * ex) {
        return Awaiter();
    }
    auto operator co_await() { return Awaiter(); }
};

namespace detail {

struct TaskPromiseBase {
    constexpr std::suspend_never initial_suspend() const noexcept { return {}; }
    constexpr std::suspend_never final_suspend() const noexcept { return {}; }
    void unhandled_exception() const noexcept {}

    void* operator new(size_t sz) noexcept {
        return ::operator new(sz);
    }
    void operator delete(void* p, size_t sz) noexcept {
        return ::operator delete(p);
    }
};

template <typename T>
struct TaskPromise : public TaskPromiseBase {
    FL_INLINE void return_value(T&& v) {
        task->setValue(std::move(v));
    }
    FL_INLINE void return_value(const T& v) { task->setValue(v); }

    TaskPromise() : task(nullptr) {}

    Task<T> get_return_object() noexcept {
        return Task<T>(Task<T>::Handle::from_promise(*this));        
    }

    Task<T>* task;
};

template <>
struct TaskPromise<void> : public TaskPromiseBase {
    FL_INLINE void return_void() const noexcept {}
    Task<void> get_return_object() noexcept { return Task<void>(); }
};

}  // namespace detail    

namespace detail {

template <typename T, bool Ready>
struct CoroTypeTraits {
};

template <typename T>
struct CoroTypeTraits<T, true> {
    using TaskType = Task<T>;
};

template <typename T>
struct CoroTypeTraits<T, false> {
    using TaskType = Lazy<T>;
};

}  // namespace detail


}  // namespace coro
}  // namespace future_lite

#endif
