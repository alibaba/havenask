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

#include "autil/mem_pool/ChunkAllocatorBase.h"

namespace autil { namespace mem_pool {

class SubPoolAllocator : public autil::mem_pool::ChunkAllocatorBase {
public:
    SubPoolAllocator(Pool *pool)
        : _pool(pool) {}
    void* doAllocate(size_t numBytes) final {
        return _pool->allocate(numBytes);
    }
    void doDeallocate(void* const addr, size_t numBytes) final {
        return _pool->deallocate(addr, numBytes);
    }
    Pool *_pool;
};

}
}
