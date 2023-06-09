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
#include "autil/mem_pool/RecyclePool.h"

#include <iosfwd>

#include "autil/TimeUtility.h"

using namespace std;
using namespace autil;

namespace autil { namespace mem_pool {
AUTIL_LOG_SETUP(autil, RecyclePool);

RecyclePool::RecyclePool(ChunkAllocatorBase* allocator, size_t chunkSize,
                         size_t alignSize)
    : Pool(allocator, chunkSize, alignSize)
    , _freeSize(0)
{
}

RecyclePool::RecyclePool(size_t chunkSize, size_t alignSize)
    : Pool(chunkSize, alignSize)
    , _freeSize(0)
{
}

RecyclePool::~RecyclePool() 
{
}

}}

