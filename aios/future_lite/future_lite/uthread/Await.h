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
#include "future_lite/Config.h"
#include "future_lite/Future.h"
#include "future_lite/Invoke.h"
#include "future_lite/uthread/internal/thread.h"
#if FUTURE_LITE_COROUTINE_AVAILABLE
#include "future_lite/coro/Lazy.h"
#endif

namespace future_lite {
namespace uthread {

#if FUTURE_LITE_COROUTINE_AVAILABLE
// class Foo {
// public:
//    lazy<T> bar(Ts&&...) {}
// };
// Foo f;
// await(ex, &Foo::bar, &f, Ts&&...);
template <class B, class Fn, class C, class... Ts>
decltype(auto) await(Executor* ex, Fn B::* fn, C* cls, Ts&&... ts) {
    using ValueType = typename std::result_of_t<decltype(fn)(C, Ts&&...)>::ValueType;
    Promise<ValueType> p;
    auto f = p.getFuture().via(ex);
    auto lazy = [p = std::move(p), fn, cls](Ts&&... ts) mutable -> coro::Lazy<> {
        auto val = co_await (*cls.*fn)(ts...);
        p.setValue(std::move(val));
        co_return;
    };
    CoroSpecStorage* parent = internal::get_parent_css();
    lazy(std::forward<Ts&&>(ts)...).setEx(ex).start([](auto&&){}, parent);
    return std::move(f).get();
}

// lazy<T> foo(Ts&&...);
// await(ex, foo, Ts&&...);
// auto lambda = [](Ts&&...) -> lazy<T> {};
// await(ex, lambda, Ts&&...);
template <class Fn, class... Ts>
decltype(auto) await(Executor* ex, Fn&& fn, Ts&&... ts) {
    using ValueType = typename std::result_of_t<decltype(fn)(Ts&&...)>::ValueType;
    Promise<ValueType> p;
    auto f = p.getFuture().via(ex);
    auto lazy = [p = std::move(p), fn = std::move(fn)](Ts&&... ts) mutable -> coro::Lazy<> {
        auto val = co_await fn(ts...);
        p.setValue(std::move(val));
        co_return;
    };
    CoroSpecStorage* parent = internal::get_parent_css();
    lazy(std::forward<Ts&&>(ts)...).setEx(ex).start([](auto&&){}, parent);
    return std::move(f).get();
}

#endif

// void foo(Promise<T>&&);
// await<T>(ex, foo);
// auto lambda = [](Promise<T>&&) {};
// await<T>(ex, lambda);
template <class T, class Fn>
T await(Executor* ex, Fn&& fn) {
    static_assert(is_invocable<decltype(fn), Promise<T>>::value,
                  "Callable of await is not support, eg: Callable(Promise<T>)");
    Promise<T> p;
    auto f = p.getFuture().via(ex);
    fn(std::move(p));
    return std::move(f).get();
}

} // namespace uthread
} // namespace future_lite
