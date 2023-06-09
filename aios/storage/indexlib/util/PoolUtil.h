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

#include "autil/mem_pool/PoolBase.h"

namespace indexlib {

#define IE_POOL_NEW_CLASS(pool, type, args...) (new ((pool)->allocate(sizeof(type))) type(args))

#define IE_POOL_NEW_VECTOR(pool, type, num) ((type*)((pool)->allocate(sizeof(type) * num)))

template <typename T>
inline T* IE_POOL_NEW_VECTOR2(autil::mem_pool::PoolBase* pool, size_t num)
{
    if (!pool) {
        return new T[num];
    }
    void* p = pool->allocate(sizeof(T) * num);
    return new (p) T[num];
}

template <typename T>
inline void IE_POOL_DELETE_VECTOR2(autil::mem_pool::PoolBase* pool, T* value, size_t num)
{
    if (!pool) {
        delete[] value;
        return;
    }
    for (size_t i = 0; i < num; ++i) {
        value[i].~T();
    }
    pool->deallocate((void*)value, sizeof(T) * num);
    value = NULL;
}

template <typename T>
inline void IE_POOL_DELETE_CLASS(autil::mem_pool::PoolBase* pool, T* value)
{
    if (value) {
        value->~T();
        pool->deallocate((void*)value, sizeof(T));
    }
}

template <typename T>
inline void IE_POOL_DELETE_VECTOR(autil::mem_pool::PoolBase* pool, T* value, size_t num)
{
    if (value) {
        pool->deallocate((void*)value, sizeof(T) * num);
    }
}

#define IE_POOL_COMPATIBLE_NEW_CLASS(pool, type, args...)                                                              \
    ((pool) ? IE_POOL_NEW_CLASS(pool, type, args) : new type(args))

template <typename T>
inline void IE_POOL_COMPATIBLE_DELETE_CLASS(autil::mem_pool::PoolBase* pool, T*& value)
{
    (pool) ? IE_POOL_DELETE_CLASS(pool, value) : (delete (value));
    value = NULL;
}

#define IE_POOL_COMPATIBLE_NEW_VECTOR(pool, type, num) (pool) ? IE_POOL_NEW_VECTOR(pool, type, num) : new type[num]

#define IE_POOL_COMPATIBLE_DELETE_VECTOR(pool, value, num)                                                             \
    if (!pool) {                                                                                                       \
        delete[] value;                                                                                                \
    } else {                                                                                                           \
        indexlib::IE_POOL_DELETE_VECTOR(pool, value, num);                                                             \
    }                                                                                                                  \
    value = NULL

} // namespace indexlib
