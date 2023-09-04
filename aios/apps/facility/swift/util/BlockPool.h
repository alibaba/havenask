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

#include <stdint.h>
#include <vector>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "swift/common/Common.h"
#include "swift/util/Block.h"

namespace swift {
namespace util {

// block pool is used to manage blocks
// we use new/delete to allocate/free block currently.
// It's thread-safe.
// NOTE: do not check input paremeter's validity, check and adjust in outside.

class BlockPool {
public:
    BlockPool(int64_t bufferSize, int64_t blockSize);
    BlockPool(BlockPool *parent, int64_t maxBufferSize, int64_t minBufferSize);
    ~BlockPool();

private:
    BlockPool(const BlockPool &);
    BlockPool &operator=(const BlockPool &);

public:
    BlockPtr allocate();
    void freeBlock(Block *block);
    void freeBlock(char *block);
    void freeUnusedBlock();
    void setReserveBlockCount(int64_t reserveCount);
    int64_t getReserveBlockCount();
    int64_t getMaxBlockCount() const { return _maxBlockCount; }
    int64_t getMinBlockCount() const { return _minBlockCount; }
    int64_t getBlockSize() const { return _blockSize; }
    int64_t getUsedBlockCount() const;
    int64_t getUnusedBlockCount() const;
    int64_t getMaxUnusedBlockCount() const;
    int64_t getParentUnusedBlockCount() const;

private:
    mutable autil::ThreadMutex _mutex;
    int64_t _maxBlockCount;
    int64_t _minBlockCount;
    int64_t _blockSize;
    int64_t _usedBlockCount;
    int64_t _reserveBlockCount;
    std::vector<char *> _bufferVec;
    BlockPool *_parentPool;
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(BlockPool);

} // namespace util
} // namespace swift
