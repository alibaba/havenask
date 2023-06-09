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

#include <future_lite/Common.h>
#if FUTURE_LITE_COROUTINE_AVAILABLE
#include <future_lite/coro/Lazy.h>
#endif
#include <future_lite/CoroSpecStorage.h>
#include <future_lite/uthread/internal/thread.h>

namespace future_lite {

template <typename T, uintptr_t KEY = 0>
class CoroSpecPtr {
public:
    struct DefaultDestructor : public Destructor {
        void operator()(void *data) override { delete static_cast<T *>(data); }
    };

    struct CustomDestructor : public Destructor {
        void (*_fn)(T *);

        explicit CustomDestructor(void (*fn)(T *)) noexcept : _fn(fn) {}

        void operator()(void *data) override {
            if (FL_LIKELY(nullptr != _fn)) {
                _fn(static_cast<T *>(data));
            }
        }
    };

    CoroSpecPtr() : _destructor(new DefaultDestructor()) {
        if (0 == KEY) {
            _varKey = reinterpret_cast<uintptr_t>(this);
        } else {
            _varKey = KEY;
        }
    }

    explicit CoroSpecPtr(void (*fn)(T *))
        : _destructor(new CustomDestructor(fn)) {
        if (0 == KEY) {
            _varKey = reinterpret_cast<uintptr_t>(this);
        } else {
            _varKey = KEY;
        }
    }

    ~CoroSpecPtr() {
        if (0 == KEY) {
            auto *activeCtx = uthread::internal::thread_impl::get();
            if (nullptr != activeCtx) {
                setCoroSpecVar(&activeCtx->storage_, _varKey, _destructor,
                            nullptr);
            }
        }
    }

    CoroSpecPtr(CoroSpecPtr const &) = delete;
    CoroSpecPtr &operator=(CoroSpecPtr const &) = delete;

    // for uthread
    T* get(bool recursive = false) noexcept {
        auto activeCtx = uthread::internal::thread_impl::get();
        if (!activeCtx) {
            return static_cast<T*>(nullptr);
        }
        void* data = getCoroSpecVar(&(activeCtx->storage_), _varKey, recursive);
        return static_cast<T*>(data);
    }

    // for uthread
    void reset(T* data) {
        auto activeCtx = uthread::internal::thread_impl::get();
        if (!activeCtx) {
            // if eat the nullptr case, may cause data pointer leak
            // call _destructor will cover user mistake
            throw std::runtime_error("CoroSpecPtr::reset(...) only in Uthread context!");
        }
        T* c = get(false);
        if (FL_LIKELY(c != data)) {
            setCoroSpecVar(&(activeCtx->storage_), _varKey, _destructor, data);
        }
    }

#if FUTURE_LITE_COROUTINE_AVAILABLE
    struct GetAwaiter {
        void *_data = nullptr;
        uintptr_t _key = 0;
        bool _recursive = false;

        GetAwaiter(uintptr_t key, bool recursive) : _key(key), _recursive(recursive) {}
        GetAwaiter(GetAwaiter &&awaiter)
            : _data(awaiter._data), _key(awaiter._key), _recursive(awaiter._recursive) {}

        GetAwaiter(GetAwaiter const &) = delete;
        GetAwaiter &operator=(GetAwaiter const &) = delete;

        bool await_ready() const noexcept { return false; }

        template <typename T1>
        bool await_suspend(
            std::coroutine_handle<future_lite::coro::detail::LazyPromise<T1>>
                continuation) noexcept {
            CoroSpecStorage *css = continuation.promise()._storage;
            if (css) {
                _data = getCoroSpecVar(css, _key, _recursive);
            }
            return false;
        }
        T *await_resume() noexcept { return static_cast<T *>(_data); }

        auto coAwait(Executor *ex) { return std::move(*this); }
    };

    struct ResetAwaiter {
        uintptr_t _key = 0;
        std::shared_ptr<Destructor> _destructor = nullptr;
        T *_data = nullptr;

    public:
        ResetAwaiter(uintptr_t key, std::shared_ptr<Destructor> destructor,
                     T *data)
            : _key(key), _destructor(destructor), _data(data) {}
        ResetAwaiter(ResetAwaiter &&awaiter)
            : _key(awaiter._key),
              _destructor(std::move(awaiter._destructor)),
              _data(awaiter._data) {}
        ResetAwaiter(ResetAwaiter const &) = delete;
        ResetAwaiter &operator=(ResetAwaiter const &) = delete;

        bool await_ready() const noexcept { return false; }

        template <typename T1>
        bool await_suspend(
            std::coroutine_handle<future_lite::coro::detail::LazyPromise<T1>>
                continuation) noexcept {
            CoroSpecStorage *css = continuation.promise()._storage;
            if (!css) {
                // if eat the nullptr case, may cause data pointer leak
                // call _destructor will cover user mistake
                std::cerr << "CoroSpecPtr::coRset(...) _storage is nullptr" << std::endl;
                std::terminate();
            }
            setCoroSpecVar(css, _key, _destructor, _data);
            return false;
        }
        void await_resume() noexcept {}
        auto coAwait(Executor *ex) { return std::move(*this); }
    };

    coro::Lazy<T*> coGet(bool recursive = false) {
        co_return co_await GetAwaiter(_varKey, recursive);
    }

    coro::Lazy<> coReset(T *data) {
        co_return co_await ResetAwaiter(_varKey, _destructor, data);
    }
#endif

private:
    static void *getCoroSpecVar(CoroSpecStorage const *css, uintptr_t key, bool recursive) {
        auto it = css->_varMap.find(key);
        if (css->_varMap.end() != it) {
            return it->second._data;
        }
        if (!recursive) {
            return nullptr;
        }
        CoroSpecStorage *tmp = css->_parent;
        while (tmp != nullptr) {
            it = tmp->_varMap.find(key);
            if (tmp->_varMap.end() != it) {
                return it->second._data;
            }
            tmp = tmp->_parent;
        }
        return nullptr;
    }

    static void setCoroSpecVar(CoroSpecStorage *css, uintptr_t key,
                             std::shared_ptr<Destructor> const &destructor,
                             void *data) {
        logicAssert(nullptr != destructor, "destructor cannot be nullptr!");
        CoroVarMap &cssMap = css->_varMap;
        auto i = cssMap.find(key);
        if (cssMap.end() != i) {
            i->second.destroy();
            i->second = CoroSpecVar{data, destructor};
        } else {
            cssMap.insert(std::make_pair(key, CoroSpecVar{data, destructor}));
        }
    }

private:
    std::shared_ptr<Destructor> _destructor;
    uintptr_t _varKey;
};

}  // namespace future_lite
