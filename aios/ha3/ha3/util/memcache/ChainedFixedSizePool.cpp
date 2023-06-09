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
#include "ha3/util/memcache/ChainedFixedSizePool.h"

#include <assert.h>
#include <string.h>
#include <algorithm>
#include <iosfwd>

#include "autil/FixedSizeAllocator.h"
#include "autil/Lock.h"
#include "autil/Log.h"

using namespace std;
using namespace autil;

namespace isearch {
namespace util {
AUTIL_LOG_SETUP(ha3, ChainedFixedSizePool);


ChainedFixedSizePool::ChainedFixedSizePool() {
    _dataBlockSize = 0;
    _blockSize = 0;
    _allocator = NULL;
}

ChainedFixedSizePool::~ChainedFixedSizePool() {
    delete _allocator;
    _allocator = NULL;
}

void ChainedFixedSizePool::init(uint32_t blockSize) {
    _dataBlockSize = blockSize;
    _blockSize = _dataBlockSize + sizeof(void*);
    _allocator = new FixedSizeAllocator(_blockSize);
}

ChainedBlockItem ChainedFixedSizePool::allocate(size_t size) {
    ChainedBlockItem item;
    if (size == 0) {
        return item;
    }
    uint32_t needBlockCount = (size - 1) / _dataBlockSize + 1; // upper
    autil::ScopedLock lock(_mutex);
    item.head = _allocator->allocate();
    void *tail = item.head;
    ChainedBlockItem::nextOf(tail) = NULL;
    for (uint32_t i = 1; i < needBlockCount; ++i) {
        void *buf = _allocator->allocate();
        ChainedBlockItem::nextOf(tail) = buf;
        tail = buf;
        ChainedBlockItem::nextOf(tail) = NULL;
    }
    return item;
}

void ChainedFixedSizePool::deallocate(ChainedBlockItem &item) {
    if (item.empty()) {
        return;
    }
    {
        autil::ScopedLock lock(_mutex);
        for (ChainedBlockItem::Iterator it = item.createIterator();
             it.hasNext(); )
        {
            void *buffer = it.getRawBuffer();
            it.next();
            _allocator->free(buffer);
        }
    }
    item.clear();
}

ChainedBlockItem ChainedFixedSizePool::createItem(
        const char *data, size_t size)
{
    ChainedBlockItem item = allocate(size);
    write(item, data, size);
    return item;
}

void ChainedFixedSizePool::write(ChainedBlockItem &item,
                                 const char *data, size_t size)
{
    uint32_t offset = 0;
    for (ChainedBlockItem::Iterator it = item.createIterator();
         it.hasNext(); it.next())
    {
        uint32_t writeSize = min((uint32_t)size - offset, _dataBlockSize);
        char *buffer = (char*)it.getDataBuffer();
        memcpy(buffer, data + offset, writeSize);
        offset += writeSize;
    }
    item.dataSize = size;
    assert(offset == size);
}

void ChainedFixedSizePool::read(
        ChainedBlockItem &item, char *data)
{
    uint32_t size = item.dataSize;
    uint32_t offset = 0;
    for (ChainedBlockItem::Iterator it = item.createIterator();
         it.hasNext(); it.next())
    {
        uint32_t writeSize = min(size - offset, _dataBlockSize);
        memcpy(data + offset, it.getDataBuffer(), writeSize);
        offset += writeSize;
    }
    assert(offset == size);
}

} // namespace util
} // namespace isearch
