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

#include "autil/BlockAllocator.h"
#include "autil/FixedSizeAllocator.h"
#include "autil/Lock.h"

namespace autil {

class SimpleBlockAllocator : public BlockAllocator
{
public:
    SimpleBlockAllocator(uint32_t blockSize);
    virtual ~SimpleBlockAllocator();

private:
    SimpleBlockAllocator(const SimpleBlockAllocator &);
    SimpleBlockAllocator& operator = (const SimpleBlockAllocator &);

public:
    /* override */ Block* allocBlock();
    /* override */ void freeBlock(Block* block);

public:
    // for testing
    size_t getAllocatedCount() const {return _blockHeaderAllocator.getCount();}

private:
    void clear();

private:
    FixedSizeAllocator _blockHeaderAllocator;
    FixedSizeAllocator _blockDataAllocator;
    ThreadMutex _blockAllocatorLock;
};

typedef std::shared_ptr<SimpleBlockAllocator> SimpleBlockAllocatorPtr;

///////////inline methods
///////////////////////////////////////////////

inline void SimpleBlockAllocator::clear()
{
    _blockHeaderAllocator.clear();
    _blockDataAllocator.clear();
}

}

