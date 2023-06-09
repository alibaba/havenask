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
#include "autil/SimpleBlockAllocator.h"

#include <new>

#include "autil/Block.h"
#include "autil/Log.h"

namespace autil {
AUTIL_DECLARE_AND_SETUP_LOGGER(autil, SimpleBlockAllocator);

SimpleBlockAllocator::SimpleBlockAllocator(uint32_t blockSize)
    : BlockAllocator(blockSize)
    , _blockHeaderAllocator(sizeof(Block))
    , _blockDataAllocator(blockSize)
{ 
}

SimpleBlockAllocator::~SimpleBlockAllocator() { 
    clear();
}

Block* SimpleBlockAllocator::allocBlock()
{
    ScopedLock lock(_blockAllocatorLock);
    Block *block = new (_blockHeaderAllocator.allocate()) Block();
    block->_data = (uint8_t *)_blockDataAllocator.allocate();
    return block;
}

void SimpleBlockAllocator::freeBlock(Block* block)
{
    ScopedLock lock(_blockAllocatorLock);
    _blockDataAllocator.free(block->_data);
    block->~Block();
    _blockHeaderAllocator.free(block);
}

}

