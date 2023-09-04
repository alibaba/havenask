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

#include "autil/Log.h"
#include "swift/common/Common.h"

namespace swift {
namespace util {
class Block;
class BlockPool;

SWIFT_TYPEDEF_PTR(Block);

// just a memory used to store message data, no meta, no dump
// can hold one or more message, or part of message,
// only for internal use
// NOTE: do not check input paremeter's validity, check and adjust in outside.

class Block {
public:
    Block(int64_t size, BlockPool *pool = NULL, char *buffer = NULL);
    ~Block();

public:
    char *getBuffer() const { return _buffer; }
    int64_t getSize() const { return _bufferSize; }
    void setNext(const BlockPtr &next) { _next = next; }
    const BlockPtr &getNext() const { return _next; }
    BlockPool *getPool() const { return _pool; }
    void setPool(BlockPool *pool) {
        assert((pool != NULL) == (_pool != NULL));
        _pool = pool;
    }
    void setActualSize(int64_t len) { _actualBufferSize = len; }
    int64_t getActualSize() { return _actualBufferSize; }

private:
    char *_buffer;
    int64_t _bufferSize;
    int64_t _actualBufferSize;
    BlockPtr _next;
    BlockPool *_pool;

private:
    Block(const Block &);
    Block &operator=(const Block &);
    AUTIL_LOG_DECLARE();
};

} // namespace util
} // namespace swift
