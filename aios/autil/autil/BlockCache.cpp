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
#include "autil/BlockCache.h"

#include <assert.h>
#include <memory>
#include <stddef.h>
#include <stdint.h>

#include "autil/Block.h"
#include "autil/BlockAllocator.h"
#include "autil/BlockLinkListNode.h"
#include "autil/FixedSizeAllocator.h"
#include "autil/Log.h"
#include "autil/ReferencedHashMap.h"
#include "autil/ReplacePolicy.h"

namespace autil {
AUTIL_DECLARE_AND_SETUP_LOGGER(autil, BlockCache);

BlockCache::BlockCache()
    : _blockNodeAllocator(sizeof(BlockLinkListNode))
    , _maxBlockCount(0)
    , _blockSize(0)
    , _totalAccessCount(0)
    , _totalHitCount(0)
    , _last1000HitCount(0) {}

BlockCache::~BlockCache() { _blockNodeAllocator.clear(); }

bool BlockCache::init(size_t maxMemory,
                      uint32_t blockSize,
                      BlockAllocatorPtr &blockAllocator,
                      ReplacePolicyPtr &replacePolicy) {
    if (blockSize == 0 || blockAllocator == NULL || replacePolicy == NULL) {
        AUTIL_LOG(ERROR, "input parameters for init is invalid, please check!");
        return false;
    }

    AUTIL_LOG(INFO, "init cache %lu, %u", maxMemory, blockSize);
    _maxBlockCount = (maxMemory / blockSize);

    uint32_t bucketCount = _maxBlockCount * 3 / 4;
    bool ret = _blockMap.init(bucketCount, bucketCount);
    if (!ret) {
        return false;
    }

    _blockAllocator = blockAllocator;
    _replacePolicy = replacePolicy;
    _blockSize = blockSize;

    return true;
}

Block *BlockCache::put(Block *block) {
    if (_blockMap.size() >= _maxBlockCount && replaceBlocks() == 0) {
        AUTIL_LOG(DEBUG, "put block to BlockCache failed");
        return NULL;
    }

    BlockLinkListNode *node = allocBlockLinkListNode();
    node->_data = block;

    BlockLinkListNode *retNode = _blockMap.findAndInsert(node);
    if (retNode == node) {
        _replacePolicy->accessBlock(node);
    } else {
        _replacePolicy->accessBlock(retNode);
        freeBlockLinkListNode(node);
    }

    return retNode->_data;
}

uint32_t BlockCache::replaceBlocks() {
    BlockLinkListNode *firstReplacedNode = NULL;
    ReplacePolicy::TryReplacePredicate tryReplace(_blockMap);
    uint32_t replacedNodeCount = _replacePolicy->replaceBlocks(tryReplace, firstReplacedNode);

    AUTIL_LOG(DEBUG, "replaced node count : %u", replacedNodeCount);

    BlockLinkListNode *node = firstReplacedNode;
    while (node) {
        BlockLinkListNode *next = (BlockLinkListNode *)node->_nextListNode;

        assert(node->_data->getRefCount() == 0);
        _blockAllocator->freeBlock(node->_data);

        freeBlockLinkListNode(node);
        node = next;
    }

    return replacedNodeCount;
}

Block *BlockCache::get(blockid_t blockId) {
    BlockLinkListNode *node = _blockMap.find(blockId);

    uint64_t index = __sync_add_and_fetch(&_totalAccessCount, (uint64_t)1);

    if (index > 1000 && _last1000Hit[(index - 1) % 1000]) {
        __sync_sub_and_fetch(&_last1000HitCount, (uint32_t)1);
    }

    if (node) {
        __sync_add_and_fetch(&_totalHitCount, (uint64_t)1);
        __sync_add_and_fetch(&_last1000HitCount, (uint32_t)1);

        _last1000Hit[(index - 1) % 1000] = true;
        _replacePolicy->accessBlock(node);
        return node->_data;
    }

    _last1000Hit[(index - 1) % 1000] = false;
    return NULL;
}

void BlockCache::setMaxMemory(size_t maxMemory) { _maxBlockCount = (maxMemory / _blockSize); }

} // namespace autil
