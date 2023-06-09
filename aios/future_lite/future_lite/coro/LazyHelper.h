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
#ifndef FUTURE_LITE_CORO_LAZY_HELPER_H
#define FUTURE_LITE_CORO_LAZY_HELPER_H

#include "future_lite/Common.h"
#include "future_lite/Invoke.h"
#include "future_lite/experimental/coroutine.h"
#include "future_lite/util/Condition.h"
#include <exception>

namespace future_lite {
namespace coro {
template <typename LazyType>
inline auto syncAwait(LazyType&& lazy) ->
    typename std::decay_t<LazyType>::ValueType {
    logicAssert(lazy._coro.operator bool(),
                "Lazy do not have a coroutine_handle");
    auto executor = lazy._coro.promise()._executor;
    if (executor) {
        if (executor) {
            logicAssert(!executor->currentThreadInExecutor(),
                        "do not sync await in the same executor with Lazy");
        }
    }

    util::Condition cond;
    using ValueType = typename std::decay_t<LazyType>::ValueType;

    Try<ValueType> value;
    auto cb = [&cond, &value](Try<ValueType> result) {
        value = std::move(result);
        cond.set();
    };
    std::move(std::forward<LazyType>(lazy)).template start<decltype(cb), false>(std::move(cb));
    cond.wait();
    return std::move(value).value();
}

template <typename LazyType>
inline auto syncAwait(LazyType&& lazy, Executor* executor) ->
    typename std::decay_t<LazyType>::ValueType {
    logicAssert(lazy._coro.operator bool(),
                "Lazy do not have a coroutine_handle");
    if (executor) {
        logicAssert(!executor->currentThreadInExecutor(),
                    "do not sync await in the same executor with Lazy");
    }

    util::Condition cond;
    using ValueType = typename std::decay_t<LazyType>::ValueType;

    Try<ValueType> value;
    auto cb = [&cond, &value](Try<ValueType> result) {
        value = std::move(result);
        cond.set();
    };
    std::move(std::forward<LazyType>(lazy)).template start<decltype(cb), false>(std::move(cb));
    cond.wait();
    return std::move(value).value();
}    

}  // namespace coro
}  // namespace future_lite

#include "future_lite/coro/Collect.h"

#endif
