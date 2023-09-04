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
#include "autil/BlockPool.h"

#include <algorithm>
#include <sys/mman.h>

#include "autil/Log.h"

namespace autil {
AUTIL_DECLARE_AND_SETUP_LOGGER(autil, BlockPool);

BlockPool::BlockPool() {
    _capacity = 0;
    _chunkSize = 0;
    _usedSize = 0;
    _freeThreshold = 0;
    _freeList = NULL;
    _freeCount = 0;
    _blockSize = 0;
}

BlockPool::~BlockPool() {
    for (std::vector<void *>::iterator it = _bigChunk.begin(); it != _bigChunk.end(); ++it) {
        int ret = munmap(*it, _chunkSize);
        assert(0 == ret);
        (void)(&ret);
    }
}

bool BlockPool::init(uint64_t capacity, uint64_t chunkSize, uint32_t blockSize, uint64_t freeThreshold) {
    if (blockSize < 8 || chunkSize > capacity || blockSize > chunkSize || freeThreshold >= capacity ||
        0 != capacity % chunkSize || 0 != chunkSize % blockSize) {
        return false;
    }
    if (_blockSize > 0 || _chunkSize > 0 || _capacity > 0 || _freeThreshold > 0) { // initialized twice
        return false;
    }
    _capacity = capacity;
    _chunkSize = chunkSize;
    _blockSize = blockSize;
    _freeThreshold = freeThreshold;
    _bigChunk.reserve(_capacity / _chunkSize);
    return true;
}

bool BlockPool::getOneBigChunk() {
    void *bigChunk = mmap(NULL, _chunkSize, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (MAP_FAILED == bigChunk) {
        return false;
    }
    if (split(bigChunk, _chunkSize)) {
        _bigChunk.push_back(bigChunk);
        return true;
    }
    int ret = munmap(bigChunk, _chunkSize);
    assert(ret);
    (void)(&ret);
    return false;
}

bool BlockPool::split(void *chunk, uint64_t chunkSize) {
    assert(NULL != chunk && chunkSize > 0 && chunkSize % _blockSize == 0);

    MemBlock *head = (MemBlock *)chunk;
    MemBlock *tail = head;
    char *cur = (char *)chunk;
    char *end = cur + chunkSize - _blockSize;
    for (cur = cur + _blockSize; cur <= end; cur += _blockSize) {
        tail->_next = (MemBlock *)cur;
        tail = tail->_next;
    }
    tail->_next = _freeList;
    _freeList = head;
    _freeCount += chunkSize / _blockSize;
    return true;
}

} // namespace autil
