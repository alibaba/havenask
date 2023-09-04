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

#include "autil/Block.h"
#include "autil/BlockLinkListNode.h"
#include "autil/DoubleLinkList.h"
#include "autil/Lock.h"
#include "autil/ReplacePolicy.h"

namespace autil {

class LRUReplacePolicy : public ReplacePolicy {
public:
    LRUReplacePolicy(uint32_t maxBlockCountPerReplacement, uint32_t maxScanCountPerReplacement);
    ~LRUReplacePolicy();

private:
    LRUReplacePolicy(const LRUReplacePolicy &);
    LRUReplacePolicy &operator=(const LRUReplacePolicy &);

public:
    /* override */ void accessBlock(BlockLinkListNode *blockNode);
    /* override */ uint32_t replaceBlocks(TryReplacePredicate &tryReplace, BlockLinkListNode *&firstReplacedBlock);

    // for test
    size_t getLRUListLength() const { return _accessQueue.getElementCount(); }
    BlockLinkListNode *getHead() const { return (BlockLinkListNode *)_accessQueue.getHead(); }

private:
    void clear();

private:
    uint32_t _maxBlockCountPerReplacement;
    uint32_t _maxScanCountPerReplacement;

    ThreadMutex _lock;
    DoubleLinkList<Block *> _accessQueue;
    friend class LRUReplacePolicyTest;
};

} // namespace autil
