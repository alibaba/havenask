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
#include <deque>
#include <xmmintrin.h>

#include "future_lite/experimental/coroutine.h"
#include "indexlib/util/Traits.h"

namespace indexlib { namespace util {

struct SchedulerQueue {
    static constexpr const int N = 32;
    using coro_handle = std::coroutine_handle<>;

    uint32_t head = 0;
    uint32_t tail = 0;
    coro_handle arr[N];

    void push_back(coro_handle h)
    {
        arr[head] = h;
        head = (head + 1) % N;
    }

    coro_handle pop_front()
    {
        auto result = arr[tail];
        tail = (tail + 1) % N;
        return result;
    }

    bool empty() { return tail == head; }
    size_t size() { return (head - tail + N) % N; }
    void clear() { head = tail = 0; }
};

// Simple thread caching allocator.
struct TCAlloc {
    struct header {
        header* next;
        size_t size;
    };
    header* root = nullptr;
    size_t last_size_allocated = 0;
    size_t total = 0;
    size_t alloc_count = 0;

    ~TCAlloc()
    {
        auto current = root;
        while (current) {
            auto next = current->next;
            ::free(current);
            current = next;
        }
    }

    void* allocate(size_t sz)
    {
        if (root && root->size <= sz) {
            void* mem = root;
            root = root->next;
            return mem;
        }
        ++alloc_count;
        total += sz;
        last_size_allocated = sz;
        return malloc(sz);
    }

    void stats() { printf("allocs %zu total %zu sz %zu\n", alloc_count, total, last_size_allocated); }

    void deallocate(void* p, size_t sz)
    {
        auto new_entry = static_cast<header*>(p);
        new_entry->size = sz;
        new_entry->next = root;
        root = new_entry;
    }
};

template <typename T>
class LazyWithoutExecutor;

// using SchedulerQueue = std::deque<std::coroutine_handle<>>;

struct VoidFunctionAwaiter {
    VoidFunctionAwaiter(std::function<void()> func) : _func(func) {}
    bool await_ready() const noexcept { return false; }

    template <typename PromiseType>
    __attribute__((__always_inline__)) std::coroutine_handle<>
    await_suspend(std::coroutine_handle<PromiseType> handle) noexcept
    {
        auto* scheduleQueue = handle.promise()._scheduleQueue;
        scheduleQueue->push_back(handle);
        _func();
        return scheduleQueue->pop_front();
    }
    void await_resume() noexcept {}
    std::function<void()> _func;
};

template <int32_t STARTEGY = _MM_HINT_NTA>
struct PrefetchAwaiter {
    template <typename T>
    PrefetchAwaiter(T* address, size_t len = 64) : _address(static_cast<void const*>(address))
                                                 , _len(len)
    {
    }
    bool await_ready() const noexcept { return false; }

    template <typename PromiseType>
    __attribute__((__always_inline__)) std::coroutine_handle<>
    await_suspend(std::coroutine_handle<PromiseType> handle) noexcept
    {
        for (size_t start = 0; start < _len; start += 64) {
            _mm_prefetch((void*)((size_t)_address + start), STARTEGY);
        }
        auto* scheduleQueue = handle.promise()._scheduleQueue;
        scheduleQueue->push_back(handle);
        return scheduleQueue->pop_front();
    }
    void await_resume() noexcept {}
    void const* _address = nullptr;
    size_t _len = 0;
};

struct SuspendAwaiter {
    bool await_ready() const noexcept { return false; }

    template <typename PromiseType>
    __attribute__((__always_inline__)) std::coroutine_handle<>
    await_suspend(std::coroutine_handle<PromiseType> handle) noexcept
    {
        auto* scheduleQueue = handle.promise()._scheduleQueue;
        scheduleQueue->push_back(handle);
        return scheduleQueue->pop_front();
    }
    void await_resume() noexcept {}
};

template <typename T, typename PromiseTypeDerived>
struct PromiseBase {
    using Handle = std::coroutine_handle<PromiseTypeDerived>;

    struct Awaiter {
        Awaiter(Handle coro) : _coro(coro) {}
        bool await_ready() const noexcept { return false; }

        template <typename P>
        std::coroutine_handle<> await_suspend(std::coroutine_handle<P> handle) noexcept
        {
            _coro.promise()._scheduleQueue = handle.promise()._scheduleQueue;
            _coro.promise()._caller = handle;
            return _coro;
        }

        template <typename T2 = T>
        std::enable_if_t<std::is_void_v<T2>, void> await_resume() noexcept
        {
            _coro.destroy();
        }

        template <typename T2 = T>
        std::enable_if_t<!std::is_void_v<T2>, T2> await_resume()
        {
            if (_coro.promise().hasException()) {
                auto exception = _coro.promise().getException();
                _coro.destroy();
                std::rethrow_exception(exception);
            }
            auto value = std::move(_coro.promise().value());
            _coro.destroy();
            return value;
        }
        Handle _coro;
    };

    struct FinalAwaiter {
        FinalAwaiter(Handle coro) : _coro(coro) {}
        bool await_ready() const noexcept { return false; }

        template <typename P>
        std::coroutine_handle<> await_suspend(std::coroutine_handle<P> handle) noexcept
        {
            assert(handle == _coro);
            auto caller = handle.promise()._caller;
            if (caller) {
                return caller;
            }
            return std::noop_coroutine();
        }

        void await_resume() noexcept {}
        Handle _coro;
    };

    auto get_return_object() { return LazyWithoutExecutor<T>(Handle::from_promise(*(PromiseTypeDerived*)this)); }
    auto initial_suspend() { return std::suspend_always {}; }

    FinalAwaiter final_suspend() noexcept { return {Handle::from_promise(*(PromiseTypeDerived*)this)}; }

    auto getException() { return ((PromiseTypeDerived*)this)->_exception; }

    // void* operator new(size_t sz) {
    //     if (unlikely(allocatorTlsCache == nullptr)) {
    //         allocatorTlsCache = std::addressof(allocator);
    //     }
    //     return allocatorTlsCache->allocate(sz);
    // }

    // void operator delete(void* p, size_t sz) {
    //     allocatorTlsCache->deallocate(p, sz);
    // }

    enum class result_type { empty, value, exception };
    result_type _resultType = result_type::empty;
    std::coroutine_handle<> _caller = nullptr;
    SchedulerQueue* _scheduleQueue = nullptr;
    // inline static thread_local TCAlloc allocator;
};

template <typename T>
struct PromiseTyped : public PromiseBase<T, PromiseTyped<T>> {
    using Base = PromiseBase<T, PromiseTyped<T>>;
    PromiseTyped() {}
    ~PromiseTyped()
    {
        switch (Base::_resultType) {
        case Base::result_type::value:
            _value.~T();
            break;
        case Base::result_type::exception:
            _exception.~exception_ptr();
            break;
        default:
            break;
        }
    }
    template <typename V, typename = std::enable_if_t<std::is_convertible_v<V&&, T>>>
    void return_value(V&& value) noexcept(std::is_nothrow_constructible_v<T, V&&>)
    {
        ::new (static_cast<void*>(std::addressof(_value))) T(std::forward<V>(value));
        Base::_resultType = Base::result_type::value;
    }

    bool hasException() const noexcept { return Base::_resultType == Base::result_type::exception; }

    T value() noexcept { return std::move(_value); }

    void unhandled_exception() noexcept
    {
        ::new (static_cast<void*>(std::addressof(_exception))) std::exception_ptr(std::current_exception());
        Base::_resultType = Base::result_type::exception;
    }

    union {
        T _value;
        std::exception_ptr _exception;
    };
};

template <>
struct PromiseTyped<void> : public PromiseBase<void, PromiseTyped<void>> {
    using Base = PromiseBase<void, PromiseTyped<void>>;
    void return_void() {}
    void unhandled_exception() noexcept
    {
        ::new (static_cast<void*>(std::addressof(_exception))) std::exception_ptr(std::current_exception());
        Base::_resultType = Base::result_type::exception;
    }
    std::exception_ptr _exception;
};

template <typename T = void>
class [[nodiscard]] LazyWithoutExecutor
{
public:
    using promise_type = PromiseTyped<T>;
    using Awaiter = typename promise_type::Awaiter;
    using Handle = std::coroutine_handle<promise_type>;

    LazyWithoutExecutor() = delete;
    LazyWithoutExecutor(LazyWithoutExecutor&& other) noexcept { _coro = std::exchange(other._coro, nullptr); }
    explicit LazyWithoutExecutor(Handle coro) : _coro(coro) {}
    ~LazyWithoutExecutor()
    {
        if (_coro) {
            _coro.destroy();
        }
    }

    void setScheduleQueue(SchedulerQueue* scheduleQueue) noexcept
    {
        _coro.promise()._scheduleQueue = scheduleQueue;
        scheduleQueue->push_back(_coro);
    }

    auto operator co_await() { return Awaiter(std::exchange(_coro, nullptr)); }

    bool hasException() noexcept { return _coro.promise().hasException(); }

    T value()
    {
        if constexpr (!std::is_void_v<T>) {
            return std::move(_coro.promise().value());
        }
    }

private:
    Handle _coro;
};

template <typename T>
std::enable_if_t<!indexlibv2::util::HasTypedefIterator<T>::value, void> RunCoro(T& lazy)
{
    SchedulerQueue que;
    lazy.setScheduleQueue(&que);
    while (!que.empty()) {
        auto front = que.pop_front();
        front.resume();
    }
}

template <typename T, size_t ParallelNum = 8, int Identifier = 0>
std::enable_if_t<indexlibv2::util::HasTypedefIterator<T>::value, void> RunCoro(T& lazies)
{
    static_assert(SchedulerQueue::N > ParallelNum);
    SchedulerQueue que;
    for (auto&& lazy : lazies) {
        if (que.size() == ParallelNum) {
            while (!que.empty()) {
                auto front = que.pop_front();
                front.resume();
            }
        }
        lazy.setScheduleQueue(&que);
    }
    while (!que.empty()) {
        auto front = que.pop_front();
        front.resume();
    }
}

template <size_t ParallelNum = 8, int Identifier = 0>
struct Throttler {
    ~Throttler() { Run(); }

    template <typename T>
    void Spawn(T&& lazy)
    {
        static_assert(SchedulerQueue::N > ParallelNum);
        if (que.size() == ParallelNum) {
            Run();
        }
        lazy.setScheduleQueue(&que);
    }

    void Run()
    {
        while (!que.empty()) {
            auto front = que.pop_front();
            front.resume();
        }
    }

    SchedulerQueue que;
};

}} // namespace indexlib::util
