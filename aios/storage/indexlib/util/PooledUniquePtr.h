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

#include <cstddef>
#include <utility>

#include "autil/mem_pool/Pool.h"

namespace indexlib { namespace util {

template <typename T>
class PooledUniquePtr
{
public:
    using ValueType = T;

public:
    PooledUniquePtr() = default;
    PooledUniquePtr(std::nullptr_t) : _ptr(nullptr), _pool(nullptr) {}
    PooledUniquePtr(T* ptr, autil::mem_pool::Pool* pool) : _ptr(ptr), _pool(pool) {}
    ~PooledUniquePtr() { destroy(); }

    template <typename T2, typename Dummy = std::enable_if_t<std::is_base_of_v<T, T2>, bool>>
    PooledUniquePtr(PooledUniquePtr<T2>&& other) : _ptr(other.get())
                                                 , _pool(other.getPool())
    {
        other.release();
    }

    PooledUniquePtr(const PooledUniquePtr&) = delete;
    PooledUniquePtr& operator=(const PooledUniquePtr&) = delete;

    PooledUniquePtr& operator=(PooledUniquePtr&& other)
    {
        if (&other != this) {
            std::swap(_ptr, other._ptr);
            std::swap(_pool, other._pool);
        }
        return *this;
    }

    T& operator*() { return *_ptr; }
    T* operator->() { return _ptr; }

    T& operator*() const { return *_ptr; }
    T* operator->() const { return _ptr; }

    void release()
    {
        _ptr = nullptr;
        _pool = nullptr;
    }
    void reset(T* ptr, autil::mem_pool::Pool* pool)
    {
        destroy();
        _ptr = ptr;
        _pool = pool;
    }
    T* get() const { return _ptr; }
    autil::mem_pool::Pool* getPool() const { return _pool; }

    explicit operator bool() const noexcept { return _ptr != nullptr; }

private:
    void destroy()
    {
        if (_ptr) {
            POOL_COMPATIBLE_DELETE_CLASS(_pool, _ptr);
            _ptr = nullptr;
            _pool = nullptr;
        }
    }

private:
    T* _ptr = nullptr;
    autil::mem_pool::Pool* _pool = nullptr;
};

template <typename V>
bool operator==(const PooledUniquePtr<V>& ptr, std::nullptr_t)
{
    return !ptr;
}

template <typename V>
bool operator==(std::nullptr_t, const PooledUniquePtr<V>& ptr)
{
    return !ptr;
}

template <typename V>
bool operator!=(const PooledUniquePtr<V>& ptr, std::nullptr_t)
{
    return !(ptr == nullptr);
}

template <typename V>
bool operator!=(std::nullptr_t, const PooledUniquePtr<V>& ptr)
{
    return !(nullptr == ptr);
}

template <typename T, typename... Args>
inline PooledUniquePtr<T> MakePooledUniquePtr(autil::mem_pool::Pool* pool, Args&&... args)
{
    auto ptr = POOL_COMPATIBLE_NEW_CLASS(pool, T, std::forward<Args>(args)...);
    assert(ptr);
    return PooledUniquePtr<T>(ptr, pool);
}

}} // namespace indexlib::util
