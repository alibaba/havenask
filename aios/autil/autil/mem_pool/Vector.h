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

#include <vector>
#include <autil/mem_pool/Pool.h>
#include <autil/DataBuffer.h>
#include <autil/mem_pool/pool_allocator.h>

namespace autil { namespace mem_pool {

template<class T>
class Vector : public std::vector<T, autil::mem_pool::pool_allocator<T>> {
 public:
    typedef std::size_t                         size_type;
    typedef T                                   value_type;
    typedef autil::mem_pool::Pool*              pool_pointer;
    typedef autil::mem_pool::pool_allocator<T>  allocator_type;
    typedef std::vector<T, allocator_type>      vector;

 public:
    explicit Vector(pool_pointer pool)
        : vector(allocator_type(pool)) {}
    explicit Vector(size_type n, pool_pointer pool)
        : vector(n, allocator_type(pool)) {}
    explicit Vector(size_type n, const value_type& val, pool_pointer pool)
        : vector(n, val, allocator_type(pool)) {}

 public:
    Vector(const Vector& x) : vector(x) {}
    Vector(const Vector& x, allocator_type& allocator)
        : vector(x, allocator) {}

 public:
    __attribute__((always_inline))
    void serialize(autil::DataBuffer& dataBuffer) const {
        serialize(dataBuffer, typename autil::SerializableTypeTraits<T>::Type());
    }
    __attribute__((always_inline))
    void deserialize(autil::DataBuffer& dataBuffer) {
        deserialize(dataBuffer, typename autil::SerializableTypeTraits<T>::Type());
    }

 private:
    __attribute__((always_inline))
    void serialize(autil::DataBuffer& dataBuffer, autil::SimpleType) const {
        uint32_t size = this->size();
        dataBuffer.write(size);
        dataBuffer.writeBytes(&(*this)[0], size * sizeof(T));
    }
    __attribute__((always_inline))
    void deserialize(autil::DataBuffer& dataBuffer, autil::SimpleType) {
        uint32_t size = 0;
        dataBuffer.read(size);
        this->resize(size);
        dataBuffer.readBytes(&(*this)[0], size * sizeof(T));
    }
    __attribute__((always_inline))
    void serialize(autil::DataBuffer& dataBuffer, autil::SerializableType) const {
        uint32_t size = this->size();
        dataBuffer.write(size);
        for (auto& it : *this) {
            dataBuffer.write(it);
        }
    }
    __attribute__((always_inline))
    void deserialize(autil::DataBuffer& dataBuffer, autil::SerializableType) {
        uint32_t size = 0;
        dataBuffer.read(size);
        this->resize(size);
        for (size_t i = 0; i < size; ++i) {
            dataBuffer.read((*this)[i]);
        }
    }
};

template<class T>
class Vector<T*> : public std::vector<T*, autil::mem_pool::pool_allocator<T*>> {
 public:
    typedef std::size_t                         size_type;
    typedef T*                                  value_type;
    typedef autil::mem_pool::Pool*              pool_pointer;
    typedef autil::mem_pool::pool_allocator<T*> allocator_type;
    typedef std::vector<T*, allocator_type>     vector;

 public:
    explicit Vector(pool_pointer pool)
        : vector(allocator_type(pool)) , _pool(pool) {}
    explicit Vector(size_type n, pool_pointer pool)
        : vector(n, allocator_type(pool)) , _pool(pool) {}
    explicit Vector(size_type n, const value_type& val, pool_pointer pool)
        : vector(n, val, allocator_type(pool)) , _pool(pool) {}
    ~Vector() {}

 public:
    Vector(const Vector& x) : vector(x) {
        _pool = x.get_allocator().pool();
    }
    Vector(const Vector& x, allocator_type& allocator)
        : vector(x, allocator) {
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
        this->resize(size);
        for (size_t i = 0; i < size; ++i) {
            T* element = POOL_NEW_CLASS(_pool, T);
            dataBuffer.read(*element);
            (*this)[i] = element;
        }
    }

 private:
    pool_pointer _pool;
};

}
}

