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
#ifndef FUTURE_LITE_CORO_UTIL_H
#define FUTURE_LITE_CORO_UTIL_H

#include <mutex>
#include <atomic>
#include "future_lite/Common.h"
#include "future_lite/Invoke.h"
#include "future_lite/experimental/coroutine.h"

namespace future_lite {

namespace coro {

namespace detail {

struct DetachedCoroutine {
    struct promise_type {
        std::suspend_never initial_suspend() noexcept { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void return_void() noexcept {}
        void unhandled_exception() {
            traceException(std::current_exception());
            std::rethrow_exception(std::current_exception());
        }
        DetachedCoroutine get_return_object() noexcept { return DetachedCoroutine(); }
        // Hint to gdb script for that there is no continuation for DetachedCoroutine.
        std::coroutine_handle<> _continuation = nullptr;
    };
};

}  // namespace detail

template <typename T>
struct ReadyAwaiter {
    ReadyAwaiter(T value) : _value(std::move(value)) {}

    bool await_ready() const noexcept { return true; }
    void await_suspend(CoroHandle<>) const noexcept {}
    T await_resume() noexcept { return std::move(_value); }

    T _value;
};

}  // namespace coro
}  // namespace future_lite



#endif
