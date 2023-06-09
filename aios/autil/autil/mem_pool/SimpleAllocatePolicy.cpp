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
#include "autil/mem_pool/SimpleAllocatePolicy.h"

#include <cstddef>

#include "autil/mem_pool/SimpleAllocator.h"

using namespace std;

namespace autil { namespace mem_pool {
AUTIL_DECLARE_AND_SETUP_LOGGER(autil, SimpleAllocatePolicy);

SimpleAllocatePolicy::SimpleAllocatePolicy(ChunkAllocatorBase* allocator,
        size_t chunkSize, bool ownAllocator)
    : _allocator(allocator)
    , _ownAllocator(ownAllocator)
    , _chunkHeader(NULL)
    , _currentChunk(NULL)
    , _chunkSize(chunkSize)
    , _usedBytes(0)
    , _totalBytes(0)
{
}

SimpleAllocatePolicy::SimpleAllocatePolicy(size_t chunkSize)
    : _allocator(new SimpleAllocator())
    , _ownAllocator(true)
    , _chunkHeader(NULL)
    , _currentChunk(NULL)
    , _chunkSize(chunkSize)
    , _usedBytes(0)
    , _totalBytes(0)
{
}

SimpleAllocatePolicy::~SimpleAllocatePolicy()
{
    release();

    if (_ownAllocator)
    {
        delete _allocator;
    }
    _allocator = NULL;
}

}}

