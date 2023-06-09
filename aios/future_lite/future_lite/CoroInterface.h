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
#ifndef FUTURE_LITE_CORO_INTERFACE_H
#define FUTURE_LITE_CORO_INTERFACE_H

#include "future_lite/Config.h"
#include <vector>

#ifdef FUTURE_LITE_USE_COROUTINES

namespace future_lite {
namespace interface {
    static constexpr bool USE_COROUTINES = true;
}
}

#define FL_COAWAIT co_await
#define FL_CORETURN co_return

#include "future_lite/coro/Lazy.h"
#include "future_lite/coro/Task.h"
#include "future_lite/coro/Generator.h"
#include "future_lite/coro/AsyncGenerator.h"
#include "future_lite/coro/Trace.h"
#include "future_lite/uthread/Await.h"

#define FL_LAZY(...) future_lite::coro::Lazy<__VA_ARGS__>
#define FL_CURRENT_EXECUTOR() future_lite::GetCurrentExecutor()

namespace future_lite{
namespace interface {

template <typename TryType>
auto& getTryValue(TryType&& t) {
    return t.value();
}

template <typename TryType>
bool tryHasError(TryType&& t) {
    return t.hasError();
}

template <typename LazyType>
inline auto syncAwait(LazyType&& lazy) ->
    typename std::decay_t<LazyType>::ValueType {
    return future_lite::coro::syncAwait(std::forward<LazyType>(lazy));
}

template <typename LazyType>
inline auto syncAwait(LazyType&& lazy, Executor* executor) ->
    typename std::decay_t<LazyType>::ValueType
{
    return future_lite::coro::syncAwait(std::forward<LazyType>(lazy), executor);
}

template <typename LazyType>
inline auto syncAwaitViaExecutor(LazyType&& lazy, Executor* executor) ->
    typename std::decay_t<LazyType>::ValueType
{
    assert(executor != nullptr);
    return future_lite::coro::syncAwait(std::forward<LazyType>(lazy).via(executor));
}

template <typename T, template<typename> typename LazyType,
          typename IAlloc = std::allocator<LazyType<T>>,
          typename OAlloc = std::allocator<Try<T>>>
inline auto collectAll(std::vector<LazyType<T>, IAlloc>&& input,
                       OAlloc out_alloc = OAlloc())
    -> future_lite::coro::Lazy<std::vector<Try<T>, OAlloc>>
{
    return future_lite::coro::collectAll(std::forward<std::vector<LazyType<T>, IAlloc>>(input), out_alloc);
}

template <typename T, template <typename> typename LazyType,
          typename IAlloc = std::allocator<LazyType<T>>,
          typename OAlloc = std::allocator<Try<T>>>
inline auto collectAllPara(std::vector<LazyType<T>, IAlloc>&& input,
                           OAlloc out_alloc = OAlloc())
    -> future_lite::coro::Lazy<std::vector<Try<T>, OAlloc>> {
    return future_lite::coro::collectAllPara(std::forward<std::vector<LazyType<T>, IAlloc>>(input), out_alloc);
}

template <typename T, template <typename> typename LazyType,
          typename IAlloc = std::allocator<LazyType<T>>,
          typename OAlloc = std::allocator<Try<T>>>
inline auto collectAllWindowed(size_t maxConcurrency,
                               bool yield,
                               std::vector<LazyType<T>, IAlloc>&& input,
                               OAlloc out_alloc = OAlloc())
    -> future_lite::coro::Lazy<std::vector<Try<T>, OAlloc>> {
        return future_lite::coro::collectAllWindowed(
                maxConcurrency, yield,std::forward<std::vector<LazyType<T>, IAlloc>>(input), out_alloc);
}

template <typename T, template <typename> typename LazyType,
          typename IAlloc = std::allocator<LazyType<T>>,
          typename OAlloc = std::allocator<Try<T>>>
inline auto collectAllWindowedPara(size_t maxConcurrency,
                                   bool yield,
                                   std::vector<LazyType<T>, IAlloc>&& input,
                                   OAlloc out_alloc = OAlloc())
    -> future_lite::coro::Lazy<std::vector<Try<T>, OAlloc>> {
        return future_lite::coro::collectAllWindowedPara(
                maxConcurrency, yield, std::forward<std::vector<LazyType<T>, IAlloc>>(input), out_alloc);
}

template <class... Ts>
decltype(auto) await(Ts&&... ts) {
    return future_lite::uthread::await(std::forward<Ts&&>(ts)...);
}

template <typename LazyType, typename F>
void awaitViaExecutor(LazyType&& lazy, Executor* executor, F&& callback) {
    assert(executor != nullptr);
    std::forward<LazyType>(lazy).via(executor).start(std::forward<F>(callback));
}

template <typename T>
using use_try_t = Try<T>;

}  // namespace interface
}  // namespace future_lite


#else // Fake Coroutines

namespace future_lite {

class Executor;

namespace interface {
static constexpr bool USE_COROUTINES = false;
}

namespace interface {
    constexpr future_lite::Executor* GetNullExecutor() { return nullptr; }
}
}

#define FL_COAWAIT
#define FL_CORETURN return
#define FL_LAZY(...) __VA_ARGS__

#define FL_TRACE(...)
#define FL_CORO_TRACE(...)

#define FL_CURRENT_EXECUTOR() future_lite::interface::GetNullExecutor()

//
namespace future_lite {
namespace interface {

template <typename T>
inline T getTryValue(T&& v) {
    return std::forward<T>(v);
}

inline void getTryValue() {}

template <typename T>
inline bool tryHasError(T&& v) {
    return false;
}

template <typename T>
inline T syncAwait(T&& v, Executor* ex = nullptr) {
    // schedule on another thread in executor?
    return std::forward<T>(v);
}

inline void syncAwait(...) {
    return;
}

template <typename T>
inline T syncAwaitViaExecutor(T&& v, Executor* executor) {
    return std::forward<T>(v);
}

template <typename T>
inline std::vector<T> collectAll(std::vector<T>&& vec) {
    return std::move(vec);
}

template <typename T, typename InAlloc, typename OutAlloc>
inline std::vector<T, InAlloc> collectAll(std::vector<T, InAlloc>&& vec, OutAlloc alloc) {
    return std::move(vec);
}

template <typename T>
inline std::vector<T> collectAllPara(std::vector<T>&& vec) {
    return std::move(vec);
}

template <typename T, typename InAlloc, typename OutAlloc>
inline std::vector<T, InAlloc> collectAllPara(std::vector<T, InAlloc>&& vec, OutAlloc alloc) {
    return std::move(vec);
}

template <typename T, typename InAlloc, typename OutAlloc>
inline std::vector<T, InAlloc> collectAllWindowed(size_t maxConcurrency, bool yield,
                                                  std::vector<T, InAlloc>&& vec, OutAlloc out_alloc) {
    return std::move(vec);
}

template <typename T, typename InAlloc, typename OutAlloc>
inline std::vector<T, InAlloc> collectAllWindowedPara(size_t maxConcurrency, bool yield,
                                                      std::vector<T, InAlloc>&& vec, OutAlloc out_alloc) {
    return std::move(vec);
}

template <class B, class Fn, class C, class... Ts>
decltype(auto) await(Executor* ex, Fn B::* fn, C* cls, Ts&&... ts) {
    return (*cls.*fn)(std::forward<Ts&&>(ts)...);
}

template <class Fn, class... Ts>
decltype(auto) await(Executor* ex, Fn&& fn, Ts&&... ts) {
    return fn(std::forward<Ts&&>(ts)...);
}

template <typename T, typename F>
void awaitViaExecutor(T&& v, Executor* executor, F&& callback) {
    callback(std::forward<T>(v));
}

template <typename T>
using use_try_t = T;

}  // namespace interface
}  // namespace future_lite

#endif

#endif
