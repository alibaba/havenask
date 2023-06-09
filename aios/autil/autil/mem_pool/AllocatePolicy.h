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
#include <memory>

#include "autil/mem_pool/MemoryChunk.h"

namespace autil { namespace mem_pool {


class AllocatePolicy
{
public:
    AllocatePolicy() {}
    virtual ~AllocatePolicy() {}
public:
        /**
     * alloc a chunk
     * @param nBytes number of bytes to allocate
     * @return allocated chunk
     */
    virtual MemoryChunk* allocate(size_t nBytes) = 0;

    /**
     * release allocated chunks
     * @return total size of chunks
     */
    virtual size_t release() = 0;

    /**
     * Reset memory to init state
     */
    virtual size_t reset() = 0;

    /**
     * clear trunk data
     */
    virtual void clear() = 0;

    /**
     * Get used chunk size
     */
    virtual size_t getUsedBytes() const = 0;

    /**
     * Get total chunk size
     */
    virtual size_t getTotalBytes() const = 0;

    /**
     * Return chunk size
     */
    virtual size_t getChunkSize() const = 0;

    /**
     * Return available chunk size, it is always
     * equal to GetChunkSize() - sizeof(ChainedMemoryChunk)
     */
    virtual size_t getAvailableChunkSize() const = 0;

    /*
     * check whether pointer in chunk.
     */
    virtual bool isInChunk(const void *ptr) const = 0;
};

}}

