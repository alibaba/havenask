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

#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <vector>

#include "autil/Lock.h"

namespace autil {
struct MemBlock {
    MemBlock *_next;
};

class BlockPool {
public:
    BlockPool();
    ~BlockPool();

private:
    BlockPool(const BlockPool &);
    BlockPool &operator=(const BlockPool &);

public:
    bool init(uint64_t capacity, uint64_t chunkSize, uint32_t blockSize, uint64_t freeThreshold);
    inline void *getBlock();
    inline void freeBlock(const std::vector<void *> &blocks);
    inline void freeBlock(void *block);
    inline uint64_t getCapacity() const;
    inline uint64_t getUsedSize() const;
    inline uint32_t getBlockSize() const;
    inline uint64_t needFreeSize() const;

private:
    bool getOneBigChunk();
    bool split(void *chunk, uint64_t chunkSize);

private:
    uint64_t _capacity;
    uint64_t _chunkSize;
    uint64_t _usedSize;
    uint64_t _freeThreshold;
    MemBlock *_freeList;
    uint32_t _freeCount;
    uint32_t _blockSize;
    ThreadMutex _lock;
    std::vector<void *> _bigChunk;

private:
    friend class BlockPoolTest;
};

inline void *BlockPool::getBlock() {
    assert(_chunkSize > 0 && _blockSize > 0 && _capacity > 0);

    ScopedLock lock(_lock);
    if (_freeCount == 0) {
        if (_chunkSize * _bigChunk.size() >= _capacity || false == getOneBigChunk()) {
            return NULL;
        }
    }

    MemBlock *block = _freeList;
    _freeList = block->_next;
    --_freeCount;
    _usedSize += _blockSize;
    return (void *)block;
}

inline void BlockPool::freeBlock(void *block) {
    assert(_chunkSize > 0 && _blockSize > 0 && _capacity > 0);
    if (NULL == block) {
        return;
    }
    MemBlock *node = (MemBlock *)block;

    ScopedLock lock(_lock);
    node->_next = _freeList;
    _freeList = node;
    ++_freeCount;
    _usedSize -= _blockSize;
}

inline void BlockPool::freeBlock(const std::vector<void *> &blocks) {
    assert(_chunkSize > 0 && _blockSize > 0 && _capacity > 0);
    MemBlock *head = NULL;
    MemBlock *tail = NULL;
    uint64_t count = 0;
    for (std::vector<void *>::const_iterator it = blocks.begin(); it != blocks.end(); ++it) {
        if (NULL != *it) {
            if (NULL == head) {
                head = (MemBlock *)(*it);
                tail = (MemBlock *)(*it);
            } else {
                tail->_next = (MemBlock *)(*it);
                tail = tail->_next;
            }
            ++count;
        }
    }

    if (NULL == head) {
        return;
    }

    ScopedLock lock(_lock);
    tail->_next = _freeList;
    _freeList = head;
    _freeCount += count;
    _usedSize -= _blockSize * count;
}

inline uint64_t BlockPool::getCapacity() const { return _capacity; }

inline uint64_t BlockPool::getUsedSize() const { return _usedSize; }

inline uint32_t BlockPool::getBlockSize() const { return _blockSize; }

inline uint64_t BlockPool::needFreeSize() const {
    return _capacity - _usedSize < _freeThreshold ? _freeThreshold + _usedSize - _capacity : 0;
}
} // namespace autil
