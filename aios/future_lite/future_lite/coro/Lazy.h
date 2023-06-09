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
#ifndef FUTURE_LITE_CORO_LAZY_H
#define FUTURE_LITE_CORO_LAZY_H

#include "future_lite/Common.h"
#include "future_lite/Invoke.h"
#include "future_lite/coro/Util.h"
#include "future_lite/experimental/coroutine.h"
#include <exception>
#include <atomic>
#include <cstdio>
#include "future_lite/Try.h"
#include <future_lite/CoroSpecStorage.h>
#include "future_lite/coro/CoAwait.h"

namespace future_lite {

class Executor;

namespace coro {

template <typename T>
class Lazy;

struct Yield {};

namespace detail {
template <typename LazyType, typename IAlloc, typename OAlloc, bool Para>
struct CollectAllAwaiter;

template <typename LazyType, typename IAlloc>
struct CollectAnyAwaiter;
}  // namespace detail

namespace detail {

class LazyPromiseBase {
public:
    struct FinalAwaiter {
        bool await_ready() const noexcept { return false; }
        template <typename PromiseType>
        auto await_suspend(std::coroutine_handle<PromiseType> h) noexcept {
            return h.promise()._continuation;
        }
        void await_resume() noexcept {}
    };

    struct YieldAwaiter {
        YieldAwaiter(Executor* executor) : _executor(executor) {}
        bool await_ready() const noexcept { return false; }
        void await_suspend(std::coroutine_handle<> handle) {
            std::function<void()> func = [h = std::move(handle)]() mutable {
                h.resume();
            };
            _executor->schedule(func);
        }
        void await_resume() noexcept {}

    private:
        Executor* _executor;
    };

public:
    LazyPromiseBase() {}
    // lazily started, coroutine will not execute until first resume() is called
    std::suspend_always initial_suspend() noexcept { return {}; }
    FinalAwaiter final_suspend() noexcept { return {}; }

    template <typename Awaitable>
    auto await_transform(Lazy<Awaitable>& lazy) {
        return lazy.coAwait(_executor, _storage);
    }

    template <typename Awaitable>
    auto await_transform(Lazy<Awaitable>&& lazy) {
        return lazy.coAwait(_executor, _storage);
    }
    template <typename Awaitable>
    auto await_transform(Awaitable&& awaitable) {
        return detail::coAwait(_executor, std::forward<Awaitable>(awaitable));
    }

    auto await_transform(CurrentExecutor) {
        return ReadyAwaiter<Executor*>(_executor);
    }
    auto await_transform(Yield) { return YieldAwaiter(_executor); }

public:
    /// IMPORTANT: field order here is important
    std::coroutine_handle<> _continuation = nullptr;
    Executor* _executor = nullptr;
    CoroSpecStorage* _storage = nullptr;
};

template <typename T>
class LazyPromise : public LazyPromiseBase {
public:
    LazyPromise() noexcept {}
    ~LazyPromise() noexcept {
        switch (_resultType) {
            case result_type::value:
                _value.~T();
                break;
            case result_type::exception:
                _exception.~exception_ptr();
                break;
            default:
                break;
        }
    }

    Lazy<T> get_return_object() noexcept;

    template<typename V,
             typename = std::enable_if_t<std::is_convertible_v<V&&, T>>>
    void return_value(V&& value)
        noexcept(std::is_nothrow_constructible_v<T, V&&>) {
        ::new (static_cast<void*>(std::addressof(_value)))
            T(std::forward<V>(value));
        _resultType = result_type::value;
    }
    void unhandled_exception() noexcept {
        ::new (static_cast<void*>(std::addressof(_exception)))
            std::exception_ptr(std::current_exception());
        _resultType = result_type::exception;
    }

public:

    T& result() & {
        if (FL_UNLIKELY(_resultType == result_type::exception)) {
            std::rethrow_exception(_exception);
        }
        assert(_resultType == result_type::value);
        return _value;
    }
    T&& result() && {
        if (FL_UNLIKELY(_resultType == result_type::exception)) {
            std::rethrow_exception(_exception);
        }
        assert(_resultType == result_type::value);
        return std::move(_value);
    }

    Try<T> tryResult() noexcept {
        if (FL_UNLIKELY(_resultType == result_type::exception)) {
            return Try<T>(_exception);
        } else {
            assert(_resultType == result_type::value);
            return Try<T>(std::move(_value));
        }
    }

    enum class result_type { empty, value, exception };
    result_type _resultType = result_type::empty;
    union {
        T _value;
        std::exception_ptr _exception;
    };
};

template <>
class LazyPromise<void> : public LazyPromiseBase {
public:
    Lazy<void> get_return_object() noexcept;
    void return_void() noexcept {}
    void unhandled_exception() noexcept {
        _exception = std::current_exception();
    }

    void result() {
        if (FL_UNLIKELY(_exception != nullptr)) {
            std::rethrow_exception(_exception);
        }
    }
    Try<void> tryResult() noexcept {
        return Try<void>(_exception);
    }

public:
    std::exception_ptr _exception{nullptr};
};

}  // namespace detail

template <typename T>
class RescheduleLazy;

namespace detail {

template <typename T>
struct LazyAwaiterBase {
    using Handle = CoroHandle<detail::LazyPromise<T>>;
    Handle _handle;

    LazyAwaiterBase(LazyAwaiterBase& other) = delete;
    LazyAwaiterBase& operator=(LazyAwaiterBase& other) = delete;

    LazyAwaiterBase(LazyAwaiterBase&& other)
        : _handle(std::exchange(other._handle, nullptr)) {}

    LazyAwaiterBase& operator=(LazyAwaiterBase&& other) {
        std::swap(_handle, other._handle);
        return *this;
    }

    LazyAwaiterBase(Handle coro) : _handle(coro) {}
    ~LazyAwaiterBase() {
        if (_handle) {
            _handle.destroy();
            _handle = nullptr;
        }
    }

    bool await_ready() const noexcept { return false; }

    template <typename T2 = T, std::enable_if_t<std::is_void_v<T2>, int> = 0>
    void awaitResume() {
        _handle.promise().result();
        _handle.destroy();
        _handle = nullptr;
    }

    template <typename T2 = T, std::enable_if_t<!std::is_void_v<T2>, int> = 0>
    T awaitResume() {
        auto r = std::move(_handle.promise()).result();
        _handle.destroy();
        _handle = nullptr;
        return r;
    }

    Try<T> awaitResumeTry() noexcept {
        Try<T> ret = std::move(_handle.promise().tryResult());
        _handle.destroy();
        _handle = nullptr;
        return ret;
    }
};

}  // namespace detail

template <typename T = void>
class [[nodiscard]] Lazy {
public:
    using promise_type = detail::LazyPromise<T>;
    using Handle = CoroHandle<promise_type>;
    using ValueType = T;

    struct AwaiterBase : public detail::LazyAwaiterBase<T> {
        using Base = detail::LazyAwaiterBase<T>;
        AwaiterBase(Handle coro) : Base(coro) {}

        __attribute__((__always_inline__))
        std::coroutine_handle<>
        await_suspend(
            std::coroutine_handle<> continuation) noexcept {
            // current coro started, caller becomes my continuation
            Base::_handle.promise()._continuation = continuation;
            return Base::_handle;
        }
    };

    struct TryAwaiter : public AwaiterBase {
        TryAwaiter(Handle coro) : AwaiterBase(coro) {}
        FL_INLINE Try<T> await_resume() noexcept {
            return AwaiterBase::awaitResumeTry();
        };
    };

    struct ValueAwaiter : public AwaiterBase {
        ValueAwaiter(Handle coro) : AwaiterBase(coro) {}
        __attribute__((__always_inline__)) T await_resume() {
            return AwaiterBase::awaitResume();
        }
    };

    ~Lazy() {
        if (_coro) {
            _coro.destroy();
            _coro = nullptr;
        }
    };
    explicit Lazy(Handle coro) : _coro(coro) {}
    Lazy(Lazy && other) : _coro(std::move(other._coro)) {
        other._coro = nullptr;
    }

    Lazy(const Lazy&) = delete;
    Lazy& operator=(const Lazy&) = delete;

    RescheduleLazy<T> via(Executor * ex)&& {
        logicAssert(_coro.operator bool(),
                    "Lazy do not have a std::coroutine_handle");
        _coro.promise()._executor = ex;
        return RescheduleLazy<T>(std::exchange(_coro, nullptr));
    }

    // set executor, do not re-schedule
    Lazy<T> setEx(Executor * ex) && {
        logicAssert(_coro.operator bool(), "Lazy do not have a std::coroutine_handle");
        _coro.promise()._executor = ex;
        return Lazy<T>(std::exchange(_coro, nullptr));
    }
    Lazy<T> setStorage(CoroSpecStorage* storage) {
        logicAssert(_coro.operator bool(), "Lazy do not have a std::coroutine_handle");
        _coro.promise()._storage = storage;
        return Lazy<T>(std::exchange(_coro, nullptr));
    }

    template <typename F, bool rethrow = true>
    void start(F && callback, CoroSpecStorage *parent = nullptr) {
        auto cb = [callback = std::forward<F>(callback)](auto&& t) mutable {
            if constexpr (rethrow) {
                if (t.hasError()) {
                    std::rethrow_exception(t.getException());
                }
            }
            callback(std::move(t));
        };
        auto launchCoro = [parent](Lazy lazy, auto cb) mutable -> detail::DetachedCoroutine {
            CoroSpecStorage css(parent);
            cb(std::move(co_await lazy.setStorage(&css).coAwaitTry()));
        };
        [[maybe_unused]] auto detached = launchCoro(std::move(*this), std::move(cb));
    }

    bool isReady() const { return !_coro || _coro.done(); }

    auto operator co_await() {
        return ValueAwaiter(std::exchange(_coro, nullptr));
    }

    auto coAwait(Executor * ex, CoroSpecStorage* storage) {
        _coro.promise()._storage = storage;
        _coro.promise()._executor = ex;
        return ValueAwaiter(std::exchange(_coro, nullptr));
    }
    auto coAwaitTry() { return TryAwaiter(std::exchange(_coro, nullptr)); }

private:
    Handle _coro;
    friend class RescheduleLazy<T>;

    template <typename C>
    friend typename std::decay_t<C>::ValueType syncAwait(C &&);

    template <typename C>
    friend typename std::decay_t<C>::ValueType syncAwait(C&&, Executor*);

    template <typename LazyType, typename IAlloc, typename OAlloc, bool Para>
    friend struct detail::CollectAllAwaiter;

    template <typename LazyType, typename IAlloc>
    friend struct detail::CollectAnyAwaiter;
};

template <typename T = void>
class [[nodiscard]] RescheduleLazy {
public:
    using ValueType = typename Lazy<T>::ValueType;
    using Handle = typename Lazy<T>::Handle;

private:
    struct AwaiterBase : public detail::LazyAwaiterBase<T> {
        using Base = detail::LazyAwaiterBase<T>;
        AwaiterBase(Handle coro) : Base(coro) {}
        inline void await_suspend(CoroHandle<> continuation) {
            auto& pr = Base::_handle.promise();
            pr._continuation = continuation;
            std::function<void()> func = [h = Base::_handle]() mutable {
                h.resume();
            };
            pr._executor->schedule(func);
        }
    };
    struct ValueAwaiter : public AwaiterBase {
        ValueAwaiter(Handle coro) : AwaiterBase(coro) {}

        __attribute__((__always_inline__)) T await_resume() {
            return AwaiterBase::awaitResume();
        }
    };

    struct TryAwaiter : public AwaiterBase {
        TryAwaiter(Handle coro) : AwaiterBase(coro) {}
        FL_INLINE Try<T> await_resume() noexcept {
            return AwaiterBase::awaitResumeTry();
        };
    };

private:
    RescheduleLazy(Handle coro) : _coro(coro) {}

public:
    RescheduleLazy(const RescheduleLazy&) = delete;
    RescheduleLazy& operator=(const RescheduleLazy&) = delete;
    RescheduleLazy(RescheduleLazy && other)
        : _coro(std::exchange(other._coro, nullptr)) {}

    auto operator co_await() noexcept {
        return ValueAwaiter(std::exchange(_coro, nullptr));
    }

    auto coAwaitTry() { return TryAwaiter(std::exchange(_coro, nullptr)); }

    RescheduleLazy<T> setStorage(CoroSpecStorage* storage) {
        logicAssert(_coro.operator bool(),
                    "RescheduleLazy do not have a std::coroutine_handle");
        _coro.promise()._storage = storage;
        return RescheduleLazy<T>(std::exchange(_coro, nullptr));
    }

    template <typename F, bool rethrow = true>
    void start(F && callback, CoroSpecStorage *parent = nullptr) {
        auto cb = [callback = std::forward<F>(callback)](auto&& t) mutable {
            if constexpr (rethrow) {
                if (t.hasError()) {
                    std::rethrow_exception(t.getException());
                }
            }
            callback(std::move(t));
        };
        auto launchCoro = [parent](RescheduleLazy lazy, auto cb) mutable -> detail::DetachedCoroutine {
            CoroSpecStorage css(parent);
            cb(std::move(co_await lazy.setStorage(&css).coAwaitTry()));
        };
        [[maybe_unused]] auto detached = launchCoro(std::move(*this), std::move(cb));
    }

private:
    Handle _coro;
    friend class Lazy<T>;

    template <typename C>
    friend typename std::decay_t<C>::ValueType syncAwait(C &&);

    template <typename LazyType, typename IAlloc, typename OAlloc, bool Para>
    friend struct detail::CollectAllAwaiter;

    template <typename LazyType, typename IAlloc>
    friend struct detail::CollectAnyAwaiter;
};

template <typename T>
inline Lazy<T> detail::LazyPromise<T>::get_return_object() noexcept {
    return Lazy<T>(Lazy<T>::Handle::from_promise(*this));
}

inline Lazy<void> detail::LazyPromise<void>::get_return_object() noexcept {
    return Lazy<void>(Lazy<void>::Handle::from_promise(*this));
}

}  // namespace coro
}  // namespace future_lite

#include "future_lite/coro/LazyHelper.h"

#endif
