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

#include <memory>

#include "indexlib/util/SimplePool.h"

namespace indexlib { namespace util {

class MemBuffer
{
public:
    MemBuffer(SimplePool* pool = NULL) : _buffer(NULL), _size(0), _simplePool(pool) {}

    MemBuffer(size_t initSize, SimplePool* pool = NULL) : _buffer(NULL), _size(0), _simplePool(pool)
    {
        Reserve(initSize);
    }

    MemBuffer(const MemBuffer& other) : _buffer(NULL), _size(0), _simplePool(other._simplePool)
    {
        Reserve(other._size);
        if (_buffer) {
            memcpy(_buffer, other._buffer, _size);
        }
    }

    ~MemBuffer() { Release(); }

public:
    void Reserve(size_t size)
    {
        if (size <= _size) {
            return;
        }

        Release();
        _buffer = Allocate(size);
        _size = size;
    }

    char* GetBuffer() { return _buffer; }
    const char* GetBuffer() const { return _buffer; }
    size_t GetBufferSize() const { return _size; }

    void Release()
    {
        if (!_buffer) {
            return;
        }

        if (_simplePool) {
            _simplePool->deallocate(_buffer, _size);
        } else {
            delete[] _buffer;
        }
        _size = 0;
        _buffer = NULL;
    }

private:
    char* Allocate(size_t size)
    {
        char* buf = NULL;
        if (_simplePool) {
            buf = (char*)_simplePool->allocate(size);
        } else {
            buf = new char[size];
        }
        return buf;
    }

private:
    char* _buffer;
    size_t _size;
    SimplePool* _simplePool;
};

typedef std::shared_ptr<MemBuffer> MemBufferPtr;
}} // namespace indexlib::util
