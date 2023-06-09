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
#ifndef FUTURE_LITE_CORO_SLEEP_H
#define FUTURE_LITE_CORO_SLEEP_H

#include "future_lite/coro/Lazy.h"
#include "future_lite/Executor.h"
#include <thread>

namespace future_lite {
namespace coro {

template <typename Rep, typename Period>
Lazy<void> sleep(std::chrono::duration<Rep, Period> dur) {
    auto ex = co_await CurrentExecutor();
    if (!ex) {
        std::this_thread::sleep_for(dur);
        co_return;
    }
    co_return co_await ex->after(std::chrono::duration_cast<Executor::Duration>(dur));
}
}
}  // namespace future_lite

#endif
