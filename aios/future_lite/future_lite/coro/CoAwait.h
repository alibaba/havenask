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
#ifndef FUTURE_LITE_CORO_CO_AWAIT_H
#define FUTURE_LITE_CORO_CO_AWAIT_H


#include "future_lite/Common.h"
#include "future_lite/Invoke.h"
#include "future_lite/Executor.h"
#include "future_lite/coro/Traits.h"
#include <assert.h>
#include <exception>

#include <mutex>
#include <atomic>
#include <utility>

namespace future_lite {
namespace coro {

namespace detail {

class ViaCoroutine {
public:
    struct promise_type {
        struct FinalAwaiter;
        promise_type(Executor* ex) : _ex(ex), _ctx(Executor::NULLCTX) {}
        ViaCoroutine get_return_object() noexcept;
        void return_void() noexcept {}
        void unhandled_exception() const noexcept { assert(false); }

        std::suspend_always initial_suspend() const noexcept { return {}; }
        FinalAwaiter final_suspend() noexcept { return FinalAwaiter(_ctx); }

        struct FinalAwaiter {
            FinalAwaiter(Executor::Context ctx) : _ctx(ctx) {}
            bool await_ready() const noexcept { return false; }

            template<typename PromiseType>
            auto await_suspend(std::coroutine_handle<PromiseType> h) noexcept {
                auto& pr = h.promise();
                // promise will remain valid across final_suspend point
                if (pr._ex) {
                    std::function<void()> func = [&pr]() {
                        pr._continuation.resume();
                    };
                    pr._ex->checkin(func, _ctx);
                } else {
                    pr._continuation.resume();
                }
            }
            void await_resume() const noexcept {}

            Executor::Context _ctx;
        };

        /// IMPORTANT: field order here is important
        std::coroutine_handle<> _continuation;
        Executor* _ex;
        Executor::Context _ctx;
    };

    ViaCoroutine(std::coroutine_handle<promise_type> coro) : _coro(coro) {}
    ~ViaCoroutine() {
        if (_coro) {
            _coro.destroy();
            _coro = nullptr;
        }
    }

    ViaCoroutine(const ViaCoroutine&) = delete;
    ViaCoroutine& operator=(const ViaCoroutine&) = delete;
    ViaCoroutine(ViaCoroutine&& other) : _coro(std::exchange(other._coro, nullptr)) {}

public:
    static ViaCoroutine create(Executor* ex) { co_return; }

public:
    void checkin() {
        auto& pr = _coro.promise();
        if (pr._ex) {
            std::function<void()> func = []() {};
            pr._ex->checkin(func, pr._ctx);
        }
    }
    std::coroutine_handle<> getWrappedContinuation(
        std::coroutine_handle<> continuation) {
        // do not call this method on a moved ViaCoroutine,
        assert(_coro);
        auto& pr = _coro.promise();
        if (pr._ex) {
            pr._ctx = pr._ex->checkout();
        }
        pr._continuation = continuation;
        return _coro;
    }

private:
    std::coroutine_handle<promise_type> _coro;
};

inline ViaCoroutine ViaCoroutine::promise_type::get_return_object() noexcept {
    return ViaCoroutine(std::coroutine_handle<ViaCoroutine::promise_type>::from_promise(*this));
}


// used by co_await non-Lazy object
template <typename Awaiter>
struct [[nodiscard]] ViaAsyncAwaiter {
    template<typename Awaitable>
    ViaAsyncAwaiter(Executor * ex, Awaitable && awaitable)
        : _ex(ex),
        _awaiter(detail::getAwaiter(std::forward<Awaitable>(awaitable))),
        _viaCoroutine(ViaCoroutine::create(ex)) {}

    using HandleType = std::coroutine_handle<>;
    using AwaitSuspendResultType = decltype(
        std::declval<Awaiter &>().await_suspend(std::declval<HandleType>()));

    bool await_ready() { return _awaiter.await_ready(); }

    AwaitSuspendResultType await_suspend(HandleType continuation) {
        if constexpr (std::is_same_v<AwaitSuspendResultType, bool>) {
            bool should_suspend =
                _awaiter.await_suspend(_viaCoroutine.getWrappedContinuation(continuation));
            // TODO: if should_suspend is false, checkout/checkin should not be called.
            if (should_suspend == false) {
                _viaCoroutine.checkin();
            }
            return should_suspend;
        } else {
            return _awaiter.await_suspend(
                _viaCoroutine.getWrappedContinuation(continuation));
        }
    }

    auto await_resume() { return _awaiter.await_resume(); }

    Executor* _ex;
    Awaiter _awaiter;
    ViaCoroutine _viaCoroutine;
}; // ViaAsyncAwaiter

template <typename Awaitable,
          std::enable_if_t<!detail::HasCoAwaitMethod<std::decay_t<Awaitable>>::value, int> = 0>
inline auto coAwait(Executor* ex, Awaitable&& awaitable) {
    using AwaiterType = std::decay_t<decltype(detail::getAwaiter(std::forward<Awaitable>(awaitable)))>;
    return ViaAsyncAwaiter<AwaiterType>(ex, std::forward<Awaitable>(awaitable));
}

template <typename Awaitable,
          std::enable_if_t<detail::HasCoAwaitMethod<std::decay_t<Awaitable>>::value, int> = 0>
inline auto coAwait(Executor* ex, Awaitable&& awaitable) {
    return std::forward<Awaitable>(awaitable).coAwait(ex);
}

}  // namespace detail

}  // namespace coro
}  // namespace future_lite

#endif
