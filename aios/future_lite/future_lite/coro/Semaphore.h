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
#ifndef FUTURE_LITE_CORO_SEMAPHORE_H
#define FUTURE_LITE_CORO_SEMAPHORE_H

#include "future_lite/Common.h"
#include "future_lite/Executor.h"
#include "future_lite/coro/ConditionVariable.h"
#include "future_lite/coro/SpinLock.h"
#include "future_lite/coro/Lazy.h"
#include "future_lite/experimental/coroutine.h"

namespace future_lite {
namespace coro {

class Semaphore {
public:
    explicit Semaphore(int count = 0) : _count(count) {
    }

    Lazy<> signal() {
        auto lock = co_await _mutex.coScopedLock();
        ++_count;
        _cv.notify();
    }

    Lazy<> wait() {
        auto lock = co_await _mutex.coScopedLock();
        co_await _cv.wait(_mutex, [&] { return _count > 0; });
        --_count;
    }

private:
    SpinLock _mutex;
    ConditionVariable<SpinLock> _cv;
    int _count;
};


}  // namespace coro
}  // namespace future_lite

#endif // FUTURE_LITE_CORO_SEMAPHORE_H
