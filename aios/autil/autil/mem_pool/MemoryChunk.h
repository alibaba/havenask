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

#include <string.h>

namespace autil { namespace mem_pool {

class MemoryChunk
{
public:
    MemoryChunk(size_t nSize = 0)
        : _size(nSize)
    {
        _pos = _holder = sizeof(MemoryChunk);
        if (_size < _pos)
        {
            _size = _pos;
        }
    }

public:
    /**
     * malloc from chunk
     * @param nBytes number of bytes to malloc
     */
    inline void* allocate(size_t nBytes)
    {
        if (_size - _pos < nBytes)
        {
            return NULL;
        }
        void* ptr = (void*)( (char*)(this) + _pos );
        _pos += nBytes;
        return ptr;
    }

    inline void *allocate(size_t nBytes, size_t align)
    {
        auto ptr = (void *)((char *)(this) + _pos);
        auto space = _size - _pos;
        auto alignedPtr = std::align(align, nBytes, ptr, space);
        if (alignedPtr) {
            _pos = (char *)alignedPtr - (char *)(this) + nBytes;
        }
        return alignedPtr;
    }

    inline bool isInChunk(const void *ptr) const {
        return ptr >= (void*)this
            && ptr < (void*)((char*)this + _pos);
    }

    /**
     * is there any space to allocate
     * @return true if no free space
     */
    inline bool isEmpty() const {return (_pos >= _size);}

    /** get total size of chunk */
    inline size_t getTotalBytes() const {return _size;}

    /** get current allocation position */
    inline size_t getPos() const {return _pos;}

    /** get free size */
    inline size_t getFreeSize() const {return _size - _pos;}

    /** get used size */
    inline size_t getUsedSize() const {return _pos - _holder;}

    /** reset chunk to free stat */
    inline void reset() {_pos = _holder;}

    /** get holder **/
    inline size_t getHolderSize() const {return _holder;}

    /** clear trunk data **/
    inline void clear() {
        if (_size <= _holder) {
            return;
        }
        auto nDataBytes = _size - _holder;
        auto p = (char*)(this) + _holder;
        memset(p, 0, nDataBytes);
    }
protected:
    size_t _size;     // size of the chunk
    size_t _pos;      // current allocation position
    size_t _holder;   // size of this object
};

class ChainedMemoryChunk : public MemoryChunk
{
public:
    ChainedMemoryChunk(size_t nSize)
        : MemoryChunk(nSize)
        , _next(NULL)
        , _prev(NULL)
    {
        _pos = _holder = sizeof(ChainedMemoryChunk);
        if (_size < _pos)
        {
            _size = _pos;
        }
    }

public:
    /** get next chunk*/
    inline ChainedMemoryChunk*  next() const { return _next; }
    inline ChainedMemoryChunk*& next() { return _next; }

    inline ChainedMemoryChunk*  prev() const { return _prev; }
    inline ChainedMemoryChunk*& prev() { return _prev; }
protected:
    ChainedMemoryChunk* _next;
    ChainedMemoryChunk* _prev;
};

}}

