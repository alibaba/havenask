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
#include <memory>

#include "autil/ChunkAllocator.h"

namespace autil {

class SimpleSegregatedAllocator : public ChunkAllocator
{
private:
    SimpleSegregatedAllocator(const SimpleSegregatedAllocator&);
    void operator=(const SimpleSegregatedAllocator&);
public:
    /** constructor */
    SimpleSegregatedAllocator();
    /** destructor */
    virtual ~SimpleSegregatedAllocator();

public:
    /**
     * Initialize and allocate memory
     */
    bool init(uint32_t requestSize, uint32_t maxRequestChunk);

    /**
     * allocate one memory chunk from this pool
     * @return allocated memory address
     */
    void* allocate(uint32_t size);

    /**
     * free memory chunk which was previously returned from alloc()
     * @param pAddr chunk to free
     */
    void free(void* const pAddr);

    /**
     * release all allocated memory
     */
    void release();

    uint32_t getRequestSize() const;

protected:
    /**
     * grow the list
     * @param min chunks
     * @return false if allocate memory failed.
     */
    virtual bool allocateChunk();

protected:
    /** 
     * get next node of pAddr
     * @param pAddr pointer
     * @return next node 
     */
    inline static void*& nextOf(void* const pAddr)
    {return *(static_cast<void**>(pAddr));}

    /**
     * segregate a chunk to partitions
     * @param pChunk memory chunk
     * @param nChunkSize size of chunk
     * @param requestedSize size of partition
     * @param pEndNode the last node to link to
     * @return first node of partitions
     */
    static void* segregate(void* pChunk, uint64_t nChunkSize, 
                           uint32_t requestedSize, void* pEndNode = NULL);

protected:
    uint64_t _chunkSize;
private:
    void* _first; ///first free node
    uint32_t _requestSize;///request size
    void * _memChunk;
};

typedef std::shared_ptr<SimpleSegregatedAllocator> SimpleSegregatedAllocatorPtr;

}

