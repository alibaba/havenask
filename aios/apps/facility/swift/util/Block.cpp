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
#include "swift/util/Block.h"

#include "swift/util/BlockPool.h"

namespace swift {
namespace util {
AUTIL_LOG_SETUP(swift, Block);

Block::Block(int64_t size, BlockPool *pool, char *buffer)
    : _buffer(buffer), _bufferSize(size), _actualBufferSize(size), _pool(pool) {
    assert(bool(_pool) == bool(_buffer));
    if (!_buffer) {
        _buffer = new char[size];
    }
}

Block::~Block() {
    if (_pool) {
        _pool->freeBlock(this);
    } else {
        SWIFT_DELETE_ARRAY(_buffer);
    }
}

} // namespace util
} // namespace swift
