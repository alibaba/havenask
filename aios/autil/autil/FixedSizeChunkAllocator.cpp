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
#include "autil/FixedSizeChunkAllocator.h"

#include <assert.h>
#include <stdlib.h>

#include "autil/Log.h"

namespace autil {
AUTIL_DECLARE_AND_SETUP_LOGGER(autil, FixedSizeChunkAllocator);

FixedSizeChunkAllocator::FixedSizeChunkAllocator() {
    _fixedSize = 0;
    _free = NULL;
    _end = NULL;
    _freeList = NULL;
    _count = 0;
    _maxRequestChunk = 0;
    _buffer = NULL;
}

FixedSizeChunkAllocator::~FixedSizeChunkAllocator() { 
    release();
}

void FixedSizeChunkAllocator::init(uint32_t requestSize, uint32_t maxRequestChunk) {
    _maxRequestChunk = maxRequestChunk;
    _fixedSize = requestSize > sizeof(void *) ? requestSize : sizeof(void *);
    _buffer = (char *)(::malloc(requestSize * maxRequestChunk));
    _end = _buffer + requestSize * maxRequestChunk;
    _free = _buffer;
}

void* FixedSizeChunkAllocator::allocate(uint32_t size) {
    void *ret;
    ++_count;
    assert(_count <= _maxRequestChunk);
    if (_freeList) {
        ret = _freeList;
        _freeList  = *static_cast<void **> (_freeList);
    } else {
        assert(_free < _end);
        ret = _free;
        _free += _fixedSize;
    }
    return ret;
}

void FixedSizeChunkAllocator::free(void* const addr) {
    --_count;
    *reinterpret_cast<void **>(addr) = _freeList;
    _freeList = addr;
}

void FixedSizeChunkAllocator::release() {
    _free = NULL;
    _end = NULL;
    _freeList = NULL;
    if (_buffer) {
        ::free(_buffer);
        _buffer = NULL;
    }
}

}

