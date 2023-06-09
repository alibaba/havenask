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
#include <new>
#include <memory>

#include "autil/Block.h"
#include "autil/BlockAllocator.h"
#include "autil/BlockLinkListNode.h"
#include "autil/FixedSizeAllocator.h"
#include "autil/Lock.h"
#include "autil/ReferencedHashMap.h"
#include "autil/ReplacePolicy.h"

namespace autil {

class BlockCache
{
public:
    BlockCache();
    virtual ~BlockCache();
private:
    BlockCache(const BlockCache &);
    BlockCache& operator = (const BlockCache &);

public:
    bool init(size_t maxMemory,
              uint32_t blockSize,
              BlockAllocatorPtr& blockAllocator,
              ReplacePolicyPtr& replacePolicy);

    /**
     * Put a Block to cache
     * @param block block to put
     * @return
     *    (1) NULL
     *        the cache is full and the ReplacePolicy failed to
     *        recycle any blocks from cache. 
     *    (2) equals to @param block,
     *        it means the put is successful, the block's reference count 
     *        is increased by one automatically.
     *    (3) doesn't equal to  @param block
     *        it means some else has put a block with the same id into the cache,
     *        so the @param block is not put successfully. The returned block is the block in the cache.
     *        Then you need release the @param block and use the returned block
     *   e.g.
     *    Block* retBlock = put(myBlock);
     *    if (retBlock == NULL)
     *    {
     *         ... // cache is full
     *    }
     *    else if (retBlock != myBlock)
     *    {
     *         release myBlock
     *         use retBlock instead.
     *    }
     *    else
     *    {
     *         use myBlock
     *    }
     *        
     */
    Block* put(Block* block);

    /**
     * Get a block from cache
     * @param blockId,  the id of the block you want to get
     * @return
     *    (1) NULL
     *        There's no such a block with @param blockId
     *    (2) NOT NULL, it's the block with  @param blockId, and the block's 
     *        referenced count is increased by one automatically.
     */
    Block* get(blockid_t blockId);

    /*
     * setMaxBlockCount is used to adjust cache's max size
     */
    void setMaxMemory(size_t maxMemory);
    uint32_t getMaxBlockCount() const { return _maxBlockCount; }
    uint32_t getBlockCount() const { return (uint32_t)_blockMap.size(); }

    uint64_t getTotalAccessCount() const { return _totalAccessCount; }
    uint64_t getTotalHitCount() const { return _totalHitCount; }
    uint32_t getLast1000HitCount() const { return _last1000HitCount; }

    BlockAllocatorPtr getBlockAllocator() const { return _blockAllocator; }
    uint32_t getBlockSize() const { return _blockSize; }

private:
    uint32_t replaceBlocks();
    BlockLinkListNode* allocBlockLinkListNode();
    void freeBlockLinkListNode(BlockLinkListNode* blockNode);

private:
    ReferencedHashMap<blockid_t, BlockLinkListNode*> _blockMap;
    BlockAllocatorPtr _blockAllocator;
    ReplacePolicyPtr _replacePolicy;

    ThreadMutex _blockNodeAllocatorLock;    
    FixedSizeAllocator _blockNodeAllocator;

    uint32_t _maxBlockCount;
    uint32_t _blockSize;

    // statistic info
    uint64_t _totalAccessCount;
    uint64_t _totalHitCount;
    uint32_t _last1000HitCount;
    bool  _last1000Hit[1000];

    friend class BlockCacheTest;
};

typedef std::shared_ptr<BlockCache> BlockCachePtr;

/////////////////////////////////////////////////////////
// inline methods

inline BlockLinkListNode* BlockCache::allocBlockLinkListNode() {
    ScopedLock lock(_blockNodeAllocatorLock);
    return new (_blockNodeAllocator.allocate()) BlockLinkListNode();
}
 
inline void BlockCache::freeBlockLinkListNode(BlockLinkListNode* blockNode) {
    ScopedLock lock(_blockNodeAllocatorLock);
    blockNode->~BlockLinkListNode();
    _blockNodeAllocator.free(blockNode);
}

}

