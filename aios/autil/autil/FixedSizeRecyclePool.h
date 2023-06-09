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

#include <stddef.h>
#include <vector>

#include "autil/SanitizerUtil.h"
namespace autil {

template <class Alloc>
class FixedSizeRecyclePool {
public:
    FixedSizeRecyclePool(size_t size, size_t numPerAlloc = 4 * 1024)
        : _numPerAlloc(numPerAlloc), _free(NULL), _end(NULL), _freeList(NULL) {
        _size = size > sizeof(void *) ? size : sizeof(void *);
        _allocated.reserve(1024);
    }

    ~FixedSizeRecyclePool() {
        for (char *buffer : _allocated) {
            SanitizerUtil::UnpoisonMemoryRegion(buffer, _size * _numPerAlloc);
            _allocator.deallocate(buffer, _size * _numPerAlloc);
        }
        _allocated.clear();
        _free = NULL;
        _end = NULL;
        _freeList = NULL;
    }

private:
    FixedSizeRecyclePool(const FixedSizeRecyclePool &);
    FixedSizeRecyclePool &operator=(const FixedSizeRecyclePool &);

public:
    void *allocate() {
        void *ret;
        if (_freeList) {
            ret = _freeList;
            SanitizerUtil::UnpoisonMemoryRegion(ret, _size);
            _freeList = *static_cast<void **>(_freeList);
        } else if (_free < _end) {
            ret = _free;
            SanitizerUtil::UnpoisonMemoryRegion(ret, _size);
            _free += _size;
        } else {
            char *buf = (char *)_allocator.allocate(_size * _numPerAlloc);
            if (buf == NULL) {
                return NULL;
            }
            SanitizerUtil::PoisonMemoryRegion(buf, _size * _numPerAlloc);
            ret = buf;
            SanitizerUtil::UnpoisonMemoryRegion(ret, _size);
            _free = buf + _size;
            _end = buf + _size * _numPerAlloc;
            _allocated.push_back(buf);
        }
        return ret;
    }

    void free(void *ptr) {
        *reinterpret_cast<void **>(ptr) = _freeList;
        _freeList = ptr;
        SanitizerUtil::PoisonMemoryRegion(ptr, _size);
    }

private:
    Alloc _allocator;

    size_t _size;
    size_t _numPerAlloc;

    char *_free;
    char *_end;
    void *_freeList;
    std::vector<char *> _allocated;
};

} // namespace autil
