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
#include "swift/util/BlockPool.h"

#include <algorithm>
#include <assert.h>
#include <memory>
#include <stddef.h>

#include "autil/TimeUtility.h"

using namespace autil;
namespace swift {
namespace util {
AUTIL_LOG_SETUP(swift, BlockPool);

BlockPool::BlockPool(int64_t bufferSize, int64_t blockSize) {
    _parentPool = NULL;
    assert(bufferSize > 0 && blockSize > 0);
    _blockSize = blockSize;
    _maxBlockCount = (bufferSize + blockSize - 1) / blockSize;
    _usedBlockCount = 0;
    _minBlockCount = 0;
    _reserveBlockCount = 0;
    _bufferVec.reserve(_maxBlockCount);
}

BlockPool::BlockPool(BlockPool *parent, int64_t maxBufferSize, int64_t minBufferSize) {
    assert(parent != NULL && maxBufferSize > 0);
    _parentPool = parent;
    _blockSize = _parentPool->getBlockSize();
    _maxBlockCount = (maxBufferSize + _blockSize - 1) / _blockSize;
    _minBlockCount = (minBufferSize + _blockSize - 1) / _blockSize;
    _reserveBlockCount = 0;
    _usedBlockCount = 0;
    _bufferVec.reserve(_maxBlockCount);
}

BlockPool::~BlockPool() {
    assert(_usedBlockCount == 0);
    if (_parentPool != NULL) {
        for (size_t i = 0; i < _bufferVec.size(); ++i) {
            _parentPool->freeBlock(_bufferVec[i]);
        }
    } else {
        for (size_t i = 0; i < _bufferVec.size(); ++i) {
            delete[] _bufferVec[i];
        }
    }
    _bufferVec.clear();
}

BlockPtr BlockPool::allocate() {
    ScopedLock lock(_mutex);
    if (_usedBlockCount >= _maxBlockCount) {
        return BlockPtr();
    }
    if (!_bufferVec.empty()) {
        char *buffer = _bufferVec.back();
        _bufferVec.pop_back();
        ++_usedBlockCount;
        return BlockPtr(new Block(_blockSize, this, buffer));
    } else if (_parentPool == NULL) {
        char *buffer = new char[_blockSize];
        _usedBlockCount++;
        return BlockPtr(new Block(_blockSize, this, buffer));
    } else {
        BlockPtr block = _parentPool->allocate();
        if (block != NULL) {
            block->setPool(this);
            _usedBlockCount++;
        }
        return block;
    }
}

void BlockPool::freeBlock(Block *block) {
    assert(this == block->getPool());
    freeBlock(block->getBuffer());
}

void BlockPool::freeBlock(char *block) {
    ScopedLock lock(_mutex);
    --_usedBlockCount;
    _bufferVec.push_back(block);
}

void BlockPool::freeUnusedBlock() {
    ScopedLock lock(_mutex);
    int64_t reserve = _reserveBlockCount;
    int64_t maxReserve = _maxBlockCount - _usedBlockCount;
    if (reserve > maxReserve) {
        reserve = maxReserve;
    }
    if (reserve < _minBlockCount - _usedBlockCount) {
        reserve = _minBlockCount - _usedBlockCount;
    }
    if (reserve < 0) {
        reserve = 0;
    }
    while ((int64_t)_bufferVec.size() > reserve) {
        char *buffer = _bufferVec.back();
        _bufferVec.pop_back();
        if (_parentPool != NULL) {
            _parentPool->freeBlock(buffer);
        } else {
            delete[] buffer;
        }
    }
}

void BlockPool::setReserveBlockCount(int64_t reserveCount) {
    ScopedLock lock(_mutex);
    _reserveBlockCount = reserveCount;
}

int64_t BlockPool::getReserveBlockCount() {
    ScopedLock lock(_mutex);
    return _reserveBlockCount;
}

int64_t BlockPool::getUsedBlockCount() const {
    ScopedLock lock(_mutex);
    return _usedBlockCount;
}

int64_t BlockPool::getUnusedBlockCount() const {
    ScopedLock lock(_mutex);
    return _bufferVec.size();
}

int64_t BlockPool::getMaxUnusedBlockCount() const {
    ScopedLock lock(_mutex);
    return _maxBlockCount - _usedBlockCount;
}

int64_t BlockPool::getParentUnusedBlockCount() const {
    return _parentPool == NULL ? 0 : _parentPool->getMaxUnusedBlockCount();
}

} // namespace util
} // namespace swift
