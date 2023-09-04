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

#include <memory>
#include <stddef.h>
#include <stdint.h>

#include "autil/Allocators.h"
#include "autil/BlockAllocator.h"
#include "autil/FixedSizeAllocator.h"
#include "autil/FixedSizeRecyclePool.h"
#include "autil/Lock.h"

namespace autil {

class MmapBlockAllocator : public BlockAllocator {
public:
    MmapBlockAllocator(uint32_t blockSize, size_t blockCount);
    ~MmapBlockAllocator();

private:
    MmapBlockAllocator(const MmapBlockAllocator &);
    MmapBlockAllocator &operator=(const MmapBlockAllocator &);

public:
    /* override */ Block *allocBlock();
    /* override */ void freeBlock(Block *block);

private:
    ThreadMutex _lock;
    FixedSizeAllocator _blockHeaderAllocator;
    FixedSizeRecyclePool<MmapAllocator> _blockDataPool;
};

typedef std::shared_ptr<MmapBlockAllocator> MmapBlockAllocatorPtr;

} // namespace autil
