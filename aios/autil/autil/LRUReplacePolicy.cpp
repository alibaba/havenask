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
#include "autil/LRUReplacePolicy.h"

#include <assert.h>

#include "autil/DoubleLinkListNode.h"
#include "autil/Log.h"

namespace autil {
AUTIL_DECLARE_AND_SETUP_LOGGER(autil, LRUReplacePolicy);

LRUReplacePolicy::LRUReplacePolicy(uint32_t maxBlockCountPerReplacement,
                                   uint32_t maxScanCountPerReplacement) 
    : _maxBlockCountPerReplacement(maxBlockCountPerReplacement)
    , _maxScanCountPerReplacement(maxScanCountPerReplacement)
{ 
    assert(maxBlockCountPerReplacement <= maxScanCountPerReplacement);
}

LRUReplacePolicy::~LRUReplacePolicy() { 
}

uint32_t LRUReplacePolicy::replaceBlocks(
        TryReplacePredicate& tryReplace,
        BlockLinkListNode*& firstReplacedBlock)
{
    uint32_t scannedCount = 0, freeCount = 0;

    ScopedLock lock(_lock);
    DoubleLinkListNode<Block*>* node = _accessQueue.getTail();
    BlockLinkListNode *lastReplacedBlock = NULL;
    while (node != NULL
           && scannedCount < _maxScanCountPerReplacement 
           && freeCount < _maxBlockCountPerReplacement)
    {
        DoubleLinkListNode<Block*>* prev = node->_prevListNode;
        if (tryReplace((BlockLinkListNode*)node))
        {
            _accessQueue.deleteNode(node);
            
            BlockLinkListNode *replacedBlock = (BlockLinkListNode*)node;
            assert(replacedBlock->getRefCount() == 0);

            if (lastReplacedBlock != NULL)
            {
                lastReplacedBlock->_nextListNode = replacedBlock;
                replacedBlock->_prevListNode = lastReplacedBlock;
                replacedBlock->_nextListNode = NULL;
            }
            else
            {
                firstReplacedBlock = replacedBlock;
                replacedBlock->_nextListNode = NULL;
                replacedBlock->_prevListNode = NULL;
            }
            lastReplacedBlock = replacedBlock;

            freeCount++;
        }
        scannedCount++;
        node = prev;
    }

    if (lastReplacedBlock) {
        lastReplacedBlock->_nextListNode = NULL;
    }
    return freeCount;
}

void LRUReplacePolicy::accessBlock(BlockLinkListNode* blockNode) {
    ScopedLock lock(_lock);
    _accessQueue.moveToHead(blockNode);
}

}

