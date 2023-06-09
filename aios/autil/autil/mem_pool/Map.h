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

#include <map>
#include <autil/mem_pool/Pool.h>
#include <autil/DataBuffer.h>
#include <autil/mem_pool/pool_allocator.h>

namespace autil { namespace mem_pool {

template<class K, class T>
class Map : public std::map<K, T, std::less<K>, autil::mem_pool::pool_allocator<std::pair<K const, T>>> {
 public:
    typedef std::size_t                                     size_type;
    typedef T                                               mapped_type;
    typedef K                                               key_type;
    typedef std::pair<K const, T>                           value_type;
    typedef autil::mem_pool::Pool*                          pool_pointer;
    typedef autil::mem_pool::pool_allocator<value_type>     allocator_type;
    typedef std::map<K, T, std::less<K>, allocator_type>    map;

 public:
    explicit Map(pool_pointer pool) : map(allocator_type(pool)) {}
    ~Map() {}

 public:
    Map(const Map& x) : map(x) {}
    Map(const Map& x, const allocator_type& allocator)
        : map(x, allocator) {}

 public:
    __attribute__((always_inline))
    void serialize(autil::DataBuffer& dataBuffer) const {
        uint32_t size = this->size();
        dataBuffer.write(size);
        for (auto& it : *this) {
            dataBuffer.write(it.first);
            dataBuffer.write(it.second);
        }
    }
    __attribute__((always_inline))
    void deserialize(autil::DataBuffer& dataBuffer) {
        uint32_t size = 0;
        dataBuffer.read(size);
        this->clear();
        for (uint32_t i = 0; i < size; ++i) {
            key_type key;
            mapped_type value;
            dataBuffer.read(key);
            dataBuffer.read(value);
            this->emplace(key, value);
        }
    }
};

template<class K, class T>
class Map<K, T*> : public std::map<K, T*, std::less<K>, autil::mem_pool::pool_allocator<std::pair<K const, T*>>> {
 public:
    typedef std::size_t                                     size_type;
    typedef T*                                              mapped_type;
    typedef K                                               key_type;
    typedef std::pair<K const, T*>                          value_type;
    typedef autil::mem_pool::Pool*                          pool_pointer;
    typedef autil::mem_pool::pool_allocator<value_type>     allocator_type;
    typedef std::map<K, T*, std::less<K>, allocator_type>   map;

 public:
    explicit Map(pool_pointer pool) : map(allocator_type(pool)), _pool(pool) {}

 public:
    Map(const Map& x) : map(x) {
        _pool = x.get_allocator().pool();
    }
    Map(const Map& x, const allocator_type& allocator)
        : map(x, allocator) {
        _pool = allocator.pool();
    }

 public:
    __attribute__((always_inline))
    void serialize(autil::DataBuffer& dataBuffer) const {
        uint32_t size = this->size();
        dataBuffer.write(size);
        for (auto& it : *this) {
            dataBuffer.write(it.first);
            dataBuffer.write(*(it.second));
        }
    }
    __attribute__((always_inline))
    void deserialize(autil::DataBuffer& dataBuffer) {
        uint32_t size = 0;
        dataBuffer.read(size);
        this->clear();
        for (uint32_t i = 0; i < size; ++i) {
            key_type key;
            mapped_type value = POOL_NEW_CLASS(_pool, T, _pool);
            dataBuffer.read(key);
            dataBuffer.read(*value);
            this->emplace(key, value);
        }
    }

 private:
    pool_pointer _pool;
};

}
}

