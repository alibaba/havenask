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

#include <set>
#include <autil/mem_pool/Pool.h>
#include <autil/DataBuffer.h>
#include <autil/mem_pool/pool_allocator.h>

namespace autil { namespace mem_pool {

template<class T>
class Set : public std::set<T, std::less<T>, autil::mem_pool::pool_allocator<T>> {
 public:
    typedef std::size_t                                 size_type;
    typedef T                                           value_type;
    typedef autil::mem_pool::Pool*                      pool_pointer;
    typedef autil::mem_pool::pool_allocator<T>          allocator_type;
    typedef std::set<T, std::less<T>, allocator_type>   set;

 public:
    explicit Set(pool_pointer pool)
        : set(allocator_type(pool)) {}
    ~Set() {}

 public:
    Set(const Set& x) : set(x) {}
    Set(const Set& x, allocator_type& allocator)
        : set(x, allocator) {}

 public:
    __attribute__((always_inline))
    void serialize(autil::DataBuffer& dataBuffer) const {
        uint32_t size = this->size();
        dataBuffer.write(size);
        for (auto& it : *this) {
            dataBuffer.write(it);
        }
    }
    __attribute__((always_inline))
    void deserialize(autil::DataBuffer& dataBuffer) {
        uint32_t size = 0;
        dataBuffer.read(size);
        this->clear();
        for (uint32_t i = 0; i < size; ++i) {
            T element;
            dataBuffer.read(element);
            this->insert(element);
        }
    }
};

template<class T>
class Set<T*> : public std::set<T*, std::less<T*>, autil::mem_pool::pool_allocator<T*>> {
 public:
    typedef std::size_t                                  size_type;
    typedef T*                                           value_type;
    typedef autil::mem_pool::Pool*                       pool_pointer;
    typedef autil::mem_pool::pool_allocator<T*>          allocator_type;
    typedef std::set<T*, std::less<T*>, allocator_type>  set;

 public:
    explicit Set(pool_pointer pool)
        : set(allocator_type(pool)) , _pool(pool) {}
    ~Set() {}

 public:
    Set(const Set& x) : set(x) {
        _pool = x.get_allocator().pool();
    }
    Set(const Set& x, allocator_type& allocator)
        : set(x, allocator) {
        _pool = allocator.pool();
    }

 public:
    __attribute__((always_inline))
    void serialize(autil::DataBuffer& dataBuffer) const {
        uint32_t size = this->size();
        dataBuffer.write(size);
        for (auto& it : *this) {
            dataBuffer.write(*it);
        }
    }
    __attribute__((always_inline))
    void deserialize(autil::DataBuffer& dataBuffer) {
        uint32_t size = 0;
        dataBuffer.read(size);
        this->clear();
        for (uint32_t i = 0; i < size; ++i) {
            T* element = POOL_NEW_CLASS(_pool, T, _pool);
            dataBuffer.read(*element);
            this->insert(element);
        }
    }

 private:
    pool_pointer _pool;
};

}
}

