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
#ifndef FUTURE_LITE_CORO_ASYNC_GENERATOR_H
#define FUTURE_LITE_CORO_ASYNC_GENERATOR_H

#include "future_lite/coro/CoAwait.h"
#include "future_lite/experimental/coroutine.h"
#include "future_lite/Invoke.h"
#include "future_lite/Traits.h"
#include "future_lite/Common.h"
#include "future_lite/coro/Util.h"
#include <cassert>
#include <exception>
#include <memory>
#include <type_traits>
#include <utility>

namespace future_lite {

class Executor;

namespace coro {

template <typename Reference, typename Value>
class AsyncGenerator;

namespace detail {

template <typename T>
class ManualLifetime {
public:
    ManualLifetime() noexcept {}
    ~ManualLifetime() {}

    template <typename... Args,
              std::enable_if_t<std::is_constructible<T, Args...>::value, int> = 0>
    void construct(Args&&... args) noexcept(
        noexcept(std::is_nothrow_constructible<T, Args...>::value)) {
        new (static_cast<void*>(std::addressof(value_))) T(static_cast<Args&&>(args)...);
    }

    void destruct() noexcept { value_.~T(); }

    const T& get() const& { return value_; }
    T& get() & { return value_; }
    const T&& get() const&& { return static_cast<const T&&>(value_); }
    T&& get() && { return static_cast<T&&>(value_); }

private:
    union {
        std::remove_const_t<T> value_;
    };
};

template <typename T>
class ManualLifetime<T&> {
public:
    ManualLifetime() noexcept : ptr_(nullptr) {}
    ~ManualLifetime() {}

    void construct(T& value) noexcept { ptr_ = std::addressof(value); }

    void destruct() noexcept { ptr_ = nullptr; }

    T& get() const noexcept { return *ptr_; }

private:
    T* ptr_;
};
template <typename T>
class ManualLifetime<T&&> {
public:
    ManualLifetime() noexcept : ptr_(nullptr) {}
    ~ManualLifetime() {}

    void construct(T&& value) noexcept { ptr_ = std::addressof(value); }

    void destruct() noexcept { ptr_ = nullptr; }

    T&& get() const noexcept { return static_cast<T&&>(*ptr_); }

private:
    T* ptr_;
};

template <>
class ManualLifetime<void> {
public:
    void construct() noexcept {}

    void destruct() noexcept {}

    void get() const noexcept {}
};

template <typename Reference, typename Value>
class AsyncGeneratorPromise {
    class YieldAwaiter {
    public:
        bool await_ready() noexcept { return false; }
        std::coroutine_handle<> await_suspend(
            std::coroutine_handle<AsyncGeneratorPromise> h) noexcept {
            return h.promise()._continuation;
        }
        void await_resume() noexcept {}
    };

public:
    ~AsyncGeneratorPromise() {
        if (_hasValue) {
            _value.destruct();
        }
    }

    AsyncGenerator<Reference, Value> get_return_object() noexcept;

    std::suspend_always initial_suspend() noexcept { return {}; }

    YieldAwaiter final_suspend() noexcept { return {}; }

    template <typename U, std::enable_if_t<!std::is_reference_v<Reference> &&
                                               std::is_convertible_v<U&&, Reference>,
                                           int> = 0>
    YieldAwaiter yield_value(U&& value) noexcept(
        std::is_nothrow_constructible_v<Reference, U>) {
        _value.construct(static_cast<U&&>(value));
        _hasValue = true;
        _executor = nullptr;
        return {};
    }

    YieldAwaiter yield_value(Reference&& value) {
        _value.construct(static_cast<Reference&&>(value));
        _hasValue = true;
        _executor = nullptr;
        return YieldAwaiter{};
    }

    void unhandled_exception() noexcept { _exception = std::current_exception(); }

    void return_void() noexcept {}

    template <typename U>
    auto await_transform(U&& value) {
        return detail::coAwait(_executor, std::forward<U>(value));
    }

    auto await_transform(CurrentExecutor) noexcept {
        return ReadyAwaiter<Executor*>(_executor);
    }

    void setExecutor(Executor* ex) noexcept {
        _executor = ex;
    }

    void setContinuation(std::coroutine_handle<> continuation) noexcept {
        _continuation = continuation;
    }

    void throwIfException() {
        if (_exception) {
            std::rethrow_exception(_exception);
        }
    }

    decltype(auto) getRvalue() noexcept {
        return std::move(_value).get();
    }

    void clearValue() noexcept {
        if (_hasValue) {
            _hasValue = false;
            _value.destruct();
        }
    }

    bool hasValue() const noexcept { return _hasValue; }

private:
    std::coroutine_handle<> _continuation;
    Executor* _executor;
    std::exception_ptr _exception;
    ManualLifetime<Reference> _value;
    bool _hasValue = false;
};

}  // namespace detail

// The AsyncGenerator class represents a sequence of asynchronously produced
// values where the values are produced by a coroutine.
//
// Values are produced by using the 'co_yield' keyword and the coroutine can
// also consume other asynchronous operations using the 'co_await' keyword.
// The end of the sequence is indicated by executing 'co_return;' either
// explicitly or by letting execution run off the end of the coroutine.
//
// Reference Type
// --------------
// The first template parameter controls the 'reference' type.
// i.e. the type returned when you dereference the iterator using operator*().
// This type is typically specified as an actual reference type.
// eg. 'const T&' (non-mutable), 'T&' (mutable)  or 'T&&' (movable) depending
// what access you want your consumers to have to the yielded values.
//
// It's also possible to specify the 'Reference' template parameter as a value
// type. In this case the generator takes a copy of the yielded value (either
// copied or move-constructed) and you get a copy of this value every time
// you dereference the iterator with '*iter'.
// This can be expensive for types that are expensive to copy, but can provide
// a small performance win for types that are cheap to copy (like built-in
// integer types).
//
// Value Type
// ----------
// The second template parameter is optional, but if specified can be used as
// the value-type that should be used to take a copy of the value returned by
// the Reference type.
// By default this type is the same as 'Reference' type stripped of qualifiers
// and references. However, in some cases it can be a different type.
// For example, if the 'Reference' type was a non-reference proxy type.
//
// Example:
//  AsyncGenerator<std::tuple<const K&, V&>, std::tuple<K, V>> getItems() {
//    auto firstMap = co_await getFirstMap();
//    for (auto&& [k, v] : firstMap) {
//      co_yield {k, v};
//    }
//    auto secondMap = co_await getSecondMap();
//    for (auto&& [k, v] : secondMap) {
//      co_yield {k, v};
//    }
//  }
//
// This is mostly useful for generic algorithms that need to take copies of
// elements of the sequence.
//
// Executor Affinity
// -----------------
// An AsyncGenerator coroutine has similar executor-affinity to that of the
// future_lite::coro::Lazy coroutine type. Every time a consumer requests a new value
// from the generator using 'co_await ++it' the generator inherits the caller's
// current executor. The coroutine will ensure that it always resumes on the
// associated executor when resuming from `co_await' expression until it hits
// the next 'co_yield' or 'co_return' statement.
// Note that the executor can potentially change at a 'co_yield' statement if
// the next element of the sequence is requested from a consumer coroutine that
// is associated with a different executor.
//
// Example: Writing an async generator.
//
//  future_lite::coro::AsyncGenerator<Record&&> getRecordsAsync() {
//    auto resultSet = executeQuery(someQuery);
//    for (;;) {
//      auto resultSetPage = co_await resultSet.nextPage();
//      if (resultSetPage.empty()) break;
//      for (auto& row : resultSetPage) {
//        co_yield Record{row.get("name"), row.get("email")};
//      }
//    }
//  }
//
// Example: Consuming items from an async generator
//
//  future_lite::coro::Lazy<void> consumer() {
//    auto records = getRecordsAsync();
//    while (auto item = co_await records.next()) {
//      auto&& record = *item;
//      process(record);
//    }
//  }
//
template <typename Reference,
          typename Value = future_lite::detail::remove_cvref_t<Reference>>
class [[nodiscard]] AsyncGenerator {
    static_assert(std::is_constructible<Value, Reference>::value,
                  "AsyncGenerator 'value_type' must be constructible from a 'reference'.");

public:
    using promise_type = detail::AsyncGeneratorPromise<Reference, Value>;

private:
    using handle_t = std::coroutine_handle<promise_type>;

public:
    using value_type = Value;
    using reference = Reference;
    using pointer = std::add_pointer_t<Reference>;

public:
    AsyncGenerator() noexcept : coro_() {}

    AsyncGenerator(AsyncGenerator&& other) noexcept : coro_(std::exchange(other.coro_, {})) {}

    ~AsyncGenerator() {
        if (coro_) {
            coro_.destroy();
            coro_ = nullptr;
        }
    }

    AsyncGenerator& operator=(AsyncGenerator&& other) noexcept {
        auto oldCoro = std::exchange(coro_, std::exchange(other.coro_, {}));
        if (oldCoro) {
            oldCoro.destroy();
            oldCoro = nullptr;
        }
        return *this;
    }

    void swap(AsyncGenerator& other) noexcept { std::swap(coro_, other.coro_); }

    class NextAwaitable;
    class NextSemiAwaitable;

    class NextResult {
    public:
        NextResult() noexcept : _hasValue(false) {}

        NextResult(NextResult&& other) noexcept : _hasValue(other._hasValue) {
            if (_hasValue) {
                _value.construct(std::move(other._value).get());
            }
        }

        ~NextResult() {
            if (_hasValue) {
                _value.destruct();
            }
        }

        NextResult& operator=(NextResult&& other) {
            if (&other != this) {
                if (has_value()) {
                    _hasValue = false;
                    _value.destruct();
                }

                if (other.has_value()) {
                    _value.construct(std::move(other._value).get());
                    _hasValue = true;
                }
            }
            return *this;
        }

        bool has_value() const noexcept { return _hasValue; }

        explicit operator bool() const noexcept { return has_value(); }

        decltype(auto) value() & {
            return _value.get();
        }

        decltype(auto) value() && {
            return std::move(_value).get();
        }

        decltype(auto) value() const& {
            return _value.get();
        }

        decltype(auto) value() const&& {
            return std::move(_value).get();
        }

        decltype(auto) operator*() & { return value(); }

        decltype(auto) operator*() && { return std::move(*this).value(); }

        decltype(auto) operator*() const& { return value(); }

        decltype(auto) operator*() const&& { return std::move(*this).value(); }

        decltype(auto) operator-> () {
            auto&& x = _value.get();
            return std::addressof(x);
        }

        decltype(auto) operator-> () const {
            auto&& x = _value.get();
            return std::addressof(x);
        }

    private:
        friend NextAwaitable;
        explicit NextResult(handle_t coro) noexcept : _hasValue(true) {
            _value.construct(coro.promise().getRvalue());
        }

        detail::ManualLifetime<Reference> _value;
        bool _hasValue = false;
    };

    class NextAwaitable {
    public:
        bool await_ready() { return !coro_; }

        handle_t await_suspend(std::coroutine_handle<> continuation) noexcept {
            auto& promise = coro_.promise();
            promise.setContinuation(continuation);
            promise.clearValue();
            return coro_;
        }

        NextResult await_resume() {
            if (!coro_) {
                return NextResult{};
            } else if (coro_.done()) {
                coro_.promise().throwIfException();
                return NextResult{};
            } else {
                return NextResult{coro_};
            }
        }

    private:
        friend NextSemiAwaitable;
        explicit NextAwaitable(handle_t coro) noexcept : coro_(coro) {}

        handle_t coro_;
    };

    class NextSemiAwaitable {
    public:
        NextAwaitable coAwait(Executor* executor) noexcept {
            if (coro_) {
                coro_.promise().setExecutor(executor);
            }
            return NextAwaitable{coro_};
        }

    private:
        friend AsyncGenerator;

        explicit NextSemiAwaitable(handle_t coro) noexcept : coro_(coro) {}

        handle_t coro_;
    };

    NextSemiAwaitable next() noexcept {
        return NextSemiAwaitable{coro_};
    }

private:
    friend class detail::AsyncGeneratorPromise<Reference, Value>;

    explicit AsyncGenerator(std::coroutine_handle<promise_type> coro) noexcept
        : coro_(coro) {}

    std::coroutine_handle<promise_type> coro_;
};

namespace detail {

template <typename Reference, typename Value>
AsyncGenerator<Reference, Value>
inline AsyncGeneratorPromise<Reference, Value>::get_return_object() noexcept {
    return AsyncGenerator<Reference, Value>{std::coroutine_handle<
        AsyncGeneratorPromise<Reference, Value>>::from_promise(*this)};
}
}  // namespace detail

}  // namespace coro
}  // namespace future_lite

#endif
