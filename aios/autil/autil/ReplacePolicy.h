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

#include "autil/Block.h"
#include "autil/BlockLinkListNode.h"
#include "autil/ReferencedHashMap.h"

namespace autil {

class ReplacePolicy {
public:
    struct TryReplacePredicate {
    public:
        TryReplacePredicate(ReferencedHashMap<blockid_t, BlockLinkListNode *> &blockMap) : _blockMap(blockMap) {}
        bool operator()(BlockLinkListNode *node) {
            bool ret = _blockMap.tryDelete((BlockLinkListNode *)node);
            return ret;
        }

    private:
        ReferencedHashMap<blockid_t, BlockLinkListNode *> &_blockMap;
    };

public:
    ReplacePolicy() {}
    virtual ~ReplacePolicy() {}

private:
    ReplacePolicy(const ReplacePolicy &);
    ReplacePolicy &operator=(const ReplacePolicy &);

public:
    virtual void accessBlock(BlockLinkListNode *blockNode) = 0;
    virtual uint32_t replaceBlocks(TryReplacePredicate &tryReplace, BlockLinkListNode *&firstReplacedBlock) = 0;
};

typedef std::shared_ptr<ReplacePolicy> ReplacePolicyPtr;

} // namespace autil
