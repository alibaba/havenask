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
#include "indexlib/util/SimplePool.h"

#include <cstddef>
#include <new>

using namespace std;

namespace indexlib { namespace util {

SimplePool::SimplePool() : _usedBytes(0), _peakOfUsedBytes(0) {}

SimplePool::~SimplePool() {}

void* SimplePool::allocate(size_t numBytes)
{
    if (numBytes == 0) {
        return NULL;
    }
    std::set_new_handler(0);
    void* p = ::operator new(numBytes);
    if (p) {
        _usedBytes += numBytes;
        if (_usedBytes > _peakOfUsedBytes) {
            _peakOfUsedBytes = _usedBytes;
        }
    }
    return p;
}

void SimplePool::deallocate(void* ptr, size_t size)
{
    ::operator delete(ptr);
    _usedBytes -= size;
}

void SimplePool::release() { _usedBytes = 0; }

size_t SimplePool::reset()
{
    size_t ret = _usedBytes;
    _usedBytes = 0;
    return ret;
}

bool SimplePool::isInPool(const void* ptr) const { return false; }
}} // namespace indexlib::util
