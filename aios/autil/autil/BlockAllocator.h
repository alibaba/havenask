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

#include <memory>
#include <stdint.h>

namespace autil {
class Block;
} // namespace autil

namespace autil {

class BlockAllocator {
public:
    BlockAllocator(uint32_t blockSize) {}
    virtual ~BlockAllocator() {}

private:
    BlockAllocator(const BlockAllocator &);
    BlockAllocator &operator=(const BlockAllocator &);

public:
    virtual Block *allocBlock() = 0;
    virtual void freeBlock(Block *block) = 0;
};

typedef std::shared_ptr<BlockAllocator> BlockAllocatorPtr;

} // namespace autil
