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

#include <type_traits>
#include "future_lite/Future.h"
#include "future_lite/Invoke.h"
#include "future_lite/uthread/Async.h"
#include "future_lite/uthread/Await.h"

namespace future_lite {
namespace uthread {

// std::vector<F> v;
// make tasks concurrent execution.
// auto res = collectAll<Launch::Schedule>(v.begin(), v.end());

// make tasks async execution in current thread.
// auto res = collectAll<Launch::Current>(v.begin(), v.end());

// the type of res is std::vector<T>, the T is user task's return type.
template <class Policy, class Iterator>
std::vector<
    typename std::enable_if<
        !std::is_void<
            invoke_result_t<
                typename std::iterator_traits<Iterator>::value_type>>::value,
        invoke_result_t<
            typename std::iterator_traits<Iterator>::value_type>>::type>
collectAll(Iterator first, Iterator last, Executor* ex = nullptr) {
    assert(std::distance(first, last) >= 0);
    static_assert(!std::is_same<Launch::Prompt, Policy>::value,
                  "collectAll not support Prompt launch policy");

    using ResultType = invoke_result_t<
        typename std::iterator_traits<Iterator>::value_type>;

    struct Context {
        std::atomic<std::size_t> tasks;
        std::vector<ResultType> result;
        Promise<std::vector<ResultType>> promise;

        Context(std::size_t n, Promise<std::vector<ResultType>>&& pr)
            : tasks(n), result(n), promise(pr) {}
    };
    auto current = getCurrentExecutor();
    if (!ex) {
        ex = current;
    }
    // FIXME: delete collectAll executor param, always use current executor
    logicAssert(ex == current, "invalid executor");
    return await<std::vector<ResultType>>(ex, [first, last, ex](Promise<std::vector<ResultType>>&& pr) mutable {
        auto n = static_cast<std::size_t>(std::distance(first, last));
        auto context = std::make_shared<Context>(n, std::move(pr));
        for (auto i = 0; first != last; ++i, ++first) {
            async<Policy>([context, i, f = std::move(*first)]() mutable {
                context->result[i] = std::move(f());
                auto lastTasks = context->tasks.fetch_sub(1u, std::memory_order_acq_rel);
                if (lastTasks == 1u) {
                    context->promise.setValue(std::move(context->result));
                }
            }, ex);
        }
    });
}

template <class Policy, class Iterator>
typename std::enable_if<
    std::is_void<
        invoke_result_t<
            typename std::iterator_traits<Iterator>::value_type>>::value,
    void>::type
collectAll(Iterator first, Iterator last, Executor* ex = nullptr) {
    assert(std::distance(first, last) >= 0);
    static_assert(!std::is_same<Launch::Prompt, Policy>::value,
                  "collectAll not support Prompt launch policy");

    struct Context {
        std::atomic<std::size_t> tasks;
        Promise<bool> promise;

        Context(std::size_t n, Promise<bool>&& pr)
            : tasks(n), promise(pr) {}
    };
    auto current = getCurrentExecutor();
    if (!ex) {
        ex = current;
    }
    logicAssert(ex == current, "invalid executor");
    await<bool>(ex, [first, last, ex](Promise<bool>&& pr) mutable {
        auto n = static_cast<std::size_t>(std::distance(first, last));
        auto context = std::make_shared<Context>(n, std::move(pr));
        for (; first != last; ++first) {
            async<Policy>([context, f = std::move(*first)]() mutable {
                f();
                auto lastTasks = context->tasks.fetch_sub(1u, std::memory_order_acq_rel);
                if (lastTasks == 1u) {
                    context->promise.setValue(true);
                }
            }, ex);
        }
    });
}

} // namespace uthread
} // namespace future_lite
