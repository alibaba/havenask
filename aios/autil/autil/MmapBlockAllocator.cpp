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
#include "autil/MmapBlockAllocator.h"

#include <new>

#include "autil/Block.h"
#include "autil/Log.h"

namespace autil {
AUTIL_DECLARE_AND_SETUP_LOGGER(autil, MmapBlockAllocator);

MmapBlockAllocator::MmapBlockAllocator(uint32_t blockSize,
                                       size_t blockCount)
    :  BlockAllocator(blockSize)
    , _blockHeaderAllocator(sizeof(Block))
    , _blockDataPool(blockSize, blockCount)
{
}

MmapBlockAllocator::~MmapBlockAllocator() { 
    _blockHeaderAllocator.clear();
}

Block* MmapBlockAllocator::allocBlock() {
    ScopedLock lock(_lock);
    Block *block = new (_blockHeaderAllocator.allocate())Block();
    block->_data = (uint8_t *)_blockDataPool.allocate();
    return block;
}

void MmapBlockAllocator::freeBlock(Block* block) {
    ScopedLock lock(_lock);
    _blockDataPool.free(block->_data);
    block->~Block();
    _blockHeaderAllocator.free(block);    
}

}

