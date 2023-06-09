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
#ifndef FUTURE_LITE_CORO_BARRIER_H
#define FUTURE_LITE_CORO_BARRIER_H

#include "future_lite/Common.h"
#include "future_lite/Invoke.h"
#include "future_lite/Executor.h"
#include "future_lite/coro/Lazy.h"
#include "future_lite/coro/Event.h"
#include "future_lite/coro/Mutex.h"
#include "future_lite/coro/Util.h"
#include <exception>
#include <vector>

namespace future_lite {

namespace coro {

class Latch {
public:
    explicit Latch(size_t count) : _event(count) {}
public:
    Lazy<> post(size_t count = 1) {
        auto awaitingCoro = _event.down(count);
        if (awaitingCoro)
          awaitingCoro.resume();
        co_return;
    }
    Lazy<> wait() {
        struct Awaiter {
            Latch& _latch;
            explicit Awaiter(Latch& latch) : _latch(latch) {}
            bool await_ready() noexcept { return false; }
            void await_suspend(CoroHandle<> coro) {
                _latch._event.setAwaitingCoro(coro);
                auto awaitingCoro = _latch._event.down();
                if (awaitingCoro)
                  awaitingCoro.resume();
                return;
            }
            void await_resume() noexcept {}
        };
        co_return co_await Awaiter(*this);
    }

private:
    detail::CountEvent _event;
}; // class Latch

class Barrier {

public:

    using CoroHandleType = std::coroutine_handle<detail::LazyPromise<void>>;

    explicit Barrier(size_t count) noexcept
        :_async_mutex(),
        _waiting_coros(),
        _ori_value(count) { }

    Barrier(const Barrier&) = delete;
    Barrier(Barrier&&) = delete;
    Barrier& operator=(const Barrier&) = delete;
    Barrier& operator=(Barrier&&) = delete;

    ~Barrier() noexcept {
        assert(_waiting_coros.empty());
    }

    struct Awaiter {
        Barrier* _barrier;
        explicit Awaiter(Barrier* barrier) noexcept
            : _barrier(barrier) { }

        bool await_ready() noexcept { return false; }

        void await_suspend(CoroHandleType coro) noexcept {
            _barrier->wait(std::move(coro));
        }

        void await_resume() noexcept {}
    }; // struct Awaiter

    auto coAwait(Executor* executor) {
        return Awaiter(this);
    }

    Lazy<void> wait() {
        co_await *this;
    }

private:

    detail::DetachedCoroutine wait(CoroHandleType coro) noexcept {
        // async lock
        co_await _async_mutex.coLock().coAwait(nullptr);;
        // save coro handle
        _waiting_coros.push_back(std::move(coro));

        // when barrier count is 0, wait() does not block any coroutine.
        // ">=" can cover this corner case.
        if (_waiting_coros.size() >= _ori_value) {
            // resume all coro
            for (auto& coro : _waiting_coros) {
                auto exec = coro.promise()._executor;
                if (exec) {
                    exec->schedule([handle = std::move(coro)]() mutable {
                                   handle.resume();
                                   });
                } else {
                    coro.resume();
                }
            }
            _waiting_coros.clear();
        } // end if

        _async_mutex.unlock();

        co_return ;
    }

    Mutex _async_mutex;
    std::vector<CoroHandleType> _waiting_coros;
    size_t _ori_value;
}; // class Barrier

}  // namespace coro
}  // namespace future_lite



#endif
