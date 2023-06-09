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

#include <string.h>

#include "autil/mem_pool/Pool.h"
#include "autil/CommonMacros.h"

namespace autil {
namespace mem_pool {

class PoolString
{
public:
    static const size_t INIT_CAPACITY = 256;
    static const size_t npos = static_cast<size_t>(-1);
public:
    typedef char* iterator;
    typedef const char* const_iterator;
public:
    PoolString(Pool* pool);
    PoolString(Pool* pool, const char* str);
    PoolString(Pool* pool, const char* str, size_t size); 
    ~PoolString();
public:
    PoolString(const PoolString& other);
    PoolString& operator = (const PoolString& other);
    PoolString& operator = (const char* str);
public:
    void push_back(char c) {
        if (unlikely(_size + 1 >= _capacity)) {
            expand();
        }
        _data[_size++] = c;
        _data[_size] = '\0';
    }
    PoolString& append(const char* str);
    PoolString& append(const char* str, size_t size);
    PoolString& append(const PoolString& other);
    PoolString& operator+= (const PoolString& other) {return append(other);}
    PoolString& operator+= (const char* str) {return append(str);}
    void reserve(size_t size) {expand(size + 1);}
    void clear() {_size = 0;_data[0] = '\0';}

    iterator begin() {return _data;}
    iterator end() {return _data + _size;}
    const_iterator begin() const {return _data;}
    const_iterator end() const {return _data + _size;}

    char& operator[] (size_t pos) {return _data[pos];}
    const char& operator[] (size_t pos) const {return _data[pos];}
    inline size_t find(const PoolString& str, size_t pos = 0) const;
    inline size_t find(const char* s, size_t pos = 0) const;
    const char* c_str() const {
        return _data;
    }
    int compare(const PoolString& other) const;
    bool operator< (const PoolString& other) const;
    bool operator== (const PoolString& other) const {
        return compare(other) == 0;
    }

    size_t size() const {return _size;}
    size_t capacity() const {return _capacity;}
    bool empty() const {return size() == 0;}
private:
    inline void reset(Pool *pool);
    inline void expand(size_t reserveSize = 0);
    inline PoolString& doAppend(const char* data, size_t len);
    static void strCopy(char* dest, const char* src, size_t len) {
        memcpy(dest, src, len);
        dest[len] = '\0';
    }
private:
    size_t _capacity;
    size_t _size; // string length exclude '\0'
    char *_data;
    Pool *_pool;
private:
    friend class PoolStringTest;
};

inline PoolString& PoolString::doAppend(const char* data, size_t len) {
    expand(_size + len + 1);
    strCopy(_data + _size, data, len);
    _size += len;
    return *this;
}

inline void PoolString::reset(Pool *pool) {
    _capacity = 0;
    _size = 0;
    _pool = pool;
    _data = NULL;
}

inline size_t PoolString::find(const PoolString& str,  size_t pos) const {
    if (pos + str._size > _size) {
        return npos;
    }
    char* ret = strstr(_data + pos, str._data);
    if (NULL == ret) {
        return npos;
    }
    return ret - _data;
}

inline size_t PoolString::find(const char* s, size_t pos) const {
    size_t len = strlen(s);
    if (pos + len > _size) {
        return npos;
    }
    char* ret = strstr(_data + pos, s);
    if (NULL == ret) {
        return npos;
    }
    return ret - _data;
}

inline void PoolString::expand(size_t reserveSize) {
    if (reserveSize != 0 && _capacity >= reserveSize) {
        return;
    }
    _capacity = (_capacity != 0 ? _capacity << 1 : INIT_CAPACITY);
    while (_capacity < reserveSize) {
        _capacity = _capacity << 1;
    }
    char *newBuffer = (char*)_pool->allocate(_capacity);
    if (_size != 0) {
        strCopy(newBuffer, _data, _size);
    }
    _pool->deallocate(_data, _capacity);
    _data = newBuffer;
}

}
}
