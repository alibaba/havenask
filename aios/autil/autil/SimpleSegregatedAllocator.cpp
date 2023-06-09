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
#include "autil/SimpleSegregatedAllocator.h"

#include <cstdlib>
#include <iosfwd>

#include "autil/Log.h"

using namespace std;
namespace autil {
AUTIL_DECLARE_AND_SETUP_LOGGER(util, SimpleSegregatedAllocator);

SimpleSegregatedAllocator::SimpleSegregatedAllocator() {
    _first = NULL;
    _requestSize = 0;
    _chunkSize = 0;
    _memChunk = NULL;
}

SimpleSegregatedAllocator::~SimpleSegregatedAllocator() {
    release();
}

bool SimpleSegregatedAllocator::init(uint32_t requestSize,
                                     uint32_t maxRequestChunk) 
{
    _requestSize = ((requestSize > sizeof(void*) ) ? requestSize : sizeof(void*));
    _chunkSize = (uint64_t)maxRequestChunk * _requestSize;
    release();
    if (!allocateChunk()) {
        return false;
    };
    return true;
}

void* SimpleSegregatedAllocator::segregate(void* pChunk, uint64_t nChunkSize, 
        uint32_t requestedSize, void* pEndNode)
{
    char* pOld = static_cast<char*>(pChunk) + 
                 ((nChunkSize - requestedSize)/requestedSize ) * requestedSize;
    nextOf(pOld) = pEndNode;///pOld point to the end node
    //only one partition
    if(pOld == pChunk) {
        return pChunk;
    }
    for(char* pIter = pOld - requestedSize; pIter != pChunk; 
        pOld = pIter,pIter -= requestedSize)
    {
        nextOf(pIter) = pOld;
    }
    nextOf(pChunk) = pOld;
    return pChunk;
}

bool SimpleSegregatedAllocator::allocateChunk() {
    void* pChunk = ::malloc(_chunkSize);
    if (!pChunk) {
        AUTIL_LOG(ERROR, "Malloc chunk: [size: %lu] FAILED", _chunkSize);
        return false;
    }
    AUTIL_LOG(TRACE3, "Malloc chunk: [%p]", pChunk);

    _memChunk = pChunk;
    _first = segregate(pChunk, _chunkSize, _requestSize, _first);
    return true;
}

void SimpleSegregatedAllocator::release() {
    _first = NULL;
    if (_memChunk) {
        ::free(_memChunk);
        _memChunk = NULL;
    }
}

void* SimpleSegregatedAllocator::allocate(uint32_t /*size*/) {
    void* const pRet = _first;
    if (!pRet)
    {
        return NULL;
    }
    _first = nextOf(_first);
    return pRet;
}

void SimpleSegregatedAllocator::free(void* const pAddr)
{
    nextOf(pAddr) = _first;
    _first = pAddr;
}

uint32_t SimpleSegregatedAllocator::getRequestSize() const  {
    return _requestSize;
}

}

