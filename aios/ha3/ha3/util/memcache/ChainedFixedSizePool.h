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

#include "autil/FixedSizeAllocator.h"
#include "autil/Lock.h"

#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace util {

class ChainedBlockItem {
public:
    ChainedBlockItem() {
        head = NULL;
        dataSize = 0;
    }
public:
    inline static void *&nextOf(void *const pAddr) {
        return *(static_cast<void**>(pAddr));
    }
public:
    class Iterator {
    public:
        Iterator(void *head) {
            _cur = head;
        }
        ~Iterator() {}

    public:
        bool hasNext() { return _cur != NULL; }
        void next() { _cur = nextOf(_cur); }
        void *getDataBuffer() {
            return (void*)((char*)_cur + sizeof(void*));
        }
        void *getRawBuffer() {
            return _cur;
        }
    private:
        void *_cur;
    };
public:
    Iterator createIterator() {
        return Iterator(head);
    }
    bool empty() const {
        return head == NULL;
    }
    void clear() {
        head = NULL;
        dataSize = 0;
    }
public:
    // do not keep tail for now.
    void *head;
    uint32_t dataSize;
};

class ChainedFixedSizePool
{
public:
    ChainedFixedSizePool();
    ~ChainedFixedSizePool();
private:
    ChainedFixedSizePool(const ChainedFixedSizePool &);
    ChainedFixedSizePool& operator=(const ChainedFixedSizePool &);
public:
    void init(uint32_t blockSize);
public:
    ChainedBlockItem allocate(size_t size);
    void deallocate(ChainedBlockItem &item);
    size_t getBlockCount() const { return _allocator->getCount(); }
    size_t getMemoryUse() const { return _allocator->getCount() * _blockSize; }
public:
    ChainedBlockItem createItem(const char *data, size_t size);
    void write(ChainedBlockItem &item,
               const char *data, size_t size);
    void read(ChainedBlockItem &item, char *data);
private:
    uint32_t _dataBlockSize;
    uint32_t _blockSize;
    autil::ThreadMutex _mutex;
    autil::FixedSizeAllocator *_allocator;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ChainedFixedSizePool> ChainedFixedSizePoolPtr;

} // namespace util
} // namespace isearch

