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
#include <bits/functexcept.h>
#include <cstddef>
#include <exception>
#include <new>
#include <utility>

#include "autil/mem_pool/PoolBase.h"
#ifndef AUTIL_POOL_ALLOCATOR
#define AUTIL_POOL_ALLOCATOR

namespace autil {
namespace mem_pool {

template<typename _Tp>
class pool_allocator
{
public:
    typedef size_t     size_type;
    typedef ptrdiff_t  difference_type;
    typedef _Tp*       pointer;
    typedef const _Tp* const_pointer;
    typedef _Tp&       reference;
    typedef const _Tp& const_reference;
    typedef _Tp        value_type;

    template<typename _Tp1>
    struct rebind
    { typedef pool_allocator<_Tp1> other; };

    pool_allocator(PoolBase *pool) throw()
        : _pool(pool) { }

    pool_allocator(const pool_allocator& other) throw()
        : _pool(other._pool) { }

    template<typename _Tp1>
    pool_allocator(const pool_allocator<_Tp1>& other) throw()
        : _pool(other._pool) { }

    ~pool_allocator() throw() { }

    pointer
    address(reference __x) const { return &__x; }

    const_pointer
    address(const_reference __x) const { return &__x; }

    // NB: __n is permitted to be 0.  The C++ standard says nothing
    // about what the return value is when __n == 0.
    pointer
    allocate(size_type __n, const void* = 0)
    {
 	if (__builtin_expect(!_pool, false))
            std::__throw_bad_alloc();
	return static_cast<_Tp*>(_pool->allocate(__n * sizeof(_Tp)));
    }

    void
    deallocate(pointer __p, size_type __n)
    {
        if (_pool) {
            _pool->deallocate((void *)__p, __n * sizeof(_Tp));
        }
    }

    size_type
    max_size() const throw()
    { return size_t(-1) / sizeof(_Tp); }

    void
    construct(pointer __p, const _Tp& __val)
    { ::new(__p) _Tp(__val); }

    void
    construct(pointer __p, _Tp&& __val)
    { ::new(__p) _Tp(std::move(__val)); }

    void
    destroy(pointer __p) { __p->~_Tp(); }

public:
    PoolBase *_pool;
};

template<typename _Tp>
inline bool
operator==(const pool_allocator<_Tp>&, const pool_allocator<_Tp>&)
{ return true; }

template<typename _Tp>
inline bool
operator!=(const pool_allocator<_Tp>&, const pool_allocator<_Tp>&)
{ return false; }

}
}

#endif
