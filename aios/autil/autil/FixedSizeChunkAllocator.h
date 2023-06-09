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
#include <stdint.h>

#include "autil/ChunkAllocator.h"

namespace autil {

class FixedSizeChunkAllocator : public ChunkAllocator
{
public:
    FixedSizeChunkAllocator();
    ~FixedSizeChunkAllocator();
private:
    FixedSizeChunkAllocator(const FixedSizeChunkAllocator &);
    FixedSizeChunkAllocator& operator = (const FixedSizeChunkAllocator &);
public:
    void init(uint32_t requestSize, uint32_t maxRequestChunk);
public:
    /* override */ void* allocate(uint32_t size);
    /* override */ void free(void* const addr);
    /* override */ void release();
private:
    size_t _fixedSize;
    char *_free;
    char *_end;
    void *_freeList;
    size_t _count;
    size_t _maxRequestChunk;
    char *_buffer;
};

}

