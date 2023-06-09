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

namespace autil { namespace mem_pool {

class ChunkAllocatorBase
{
public:
    ChunkAllocatorBase(): _usedBytes(0) {}
    virtual ~ChunkAllocatorBase() {}

public:
    void* allocate(size_t numBytes) 
    {
        void *p = doAllocate(numBytes);
        _usedBytes += numBytes;
        return p;
    }
    void deallocate(void* const addr, size_t numBytes)
    {
        doDeallocate(addr, numBytes);
        _usedBytes -= numBytes;
    }
    size_t getUsedBytes() const {return _usedBytes;}

private:
    virtual void* doAllocate(size_t numBytes) = 0;
    virtual void doDeallocate(void* const addr, size_t numBytes) = 0;

protected:
    size_t _usedBytes;
};

}}

