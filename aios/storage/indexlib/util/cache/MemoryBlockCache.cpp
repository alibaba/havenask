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
#include "indexlib/util/cache/MemoryBlockCache.h"

#include "autil/MemUtil.h"         // for memory debug
#include "autil/cache/lru_cache.h" // for TEST_GetRefCount
#include "indexlib/util/cache/BlockAllocator.h"
#include "indexlib/util/cache/CacheType.h"
using namespace autil;
using namespace std;

namespace indexlib { namespace util {
AUTIL_LOG_SETUP(indexlib.util, MemoryBlockCache);

MemoryBlockCache::MemoryBlockCache() {}

MemoryBlockCache::~MemoryBlockCache()
{
    if (_cache) {
        _cache->EraseUnRefEntries();
    }
}

void DeleteBlock(const autil::StringView& key, void* value, const CacheAllocatorPtr& allocator)
{
    allocator->Deallocate(value);
}

bool MemoryBlockCache::DoInit(const BlockCacheOption& cacheOption)
{
    if (cacheOption.memorySize == 0) {
        AUTIL_LOG(WARN, "block cache disabled");
        return true;
    }
    int32_t shardBitsNum = BlockCache::DEFAULT_SHARED_BITS_NUM;
    float lruHighPriorityRatio = 0.0f;
    float lruLowPriorityRatio = 0.0f;
    if (!ExtractCacheParam(cacheOption, shardBitsNum, lruHighPriorityRatio, lruLowPriorityRatio)) {
        return false;
    }

    if (cacheOption.memorySize < ((size_t)cacheOption.blockSize << shardBitsNum)) {
        _memorySize = cacheOption.blockSize << shardBitsNum;
        AUTIL_LOG(WARN, "memorySize[%lu] small than blockSize[%lu] << %d, adjust to [%lu]", cacheOption.memorySize,
                  cacheOption.blockSize, shardBitsNum, _memorySize);
    }
    auto cacheType = GetCacheTypeFromStr(cacheOption.cacheType);
    if (cacheType == UNKNOWN) {
        AUTIL_LOG(ERROR, "unknown cache type [%s]", cacheOption.cacheType.c_str());
        return false;
    }
    assert(cacheType == LRU);
    _cache =
        NewLRUCache(_memorySize, shardBitsNum, false, lruHighPriorityRatio, lruLowPriorityRatio, GetBlockAllocator());
    if (!_cache) {
        AUTIL_LOG(ERROR,
                  "create new lru cache fail, memorySize [%lu], shardBitsNum [%d], lruHighPriorityRatio [%f] "
                  "lruLowPriorityRatio [%f]",
                  _memorySize, shardBitsNum, lruHighPriorityRatio, lruLowPriorityRatio);
        return false;
    }
    return true;
}

bool MemoryBlockCache::Put(Block* block, CacheBase::Handle** handle, autil::CacheBase::Priority priority) noexcept
{
    if (_memorySize == 0) {
        if (handle) {
            *handle = reinterpret_cast<CacheBase::Handle*>(block);
        }
        return true;
    }
    assert(_cache && block && handle);
    autil::StringView key(reinterpret_cast<const char*>(&block->id), sizeof(block->id));
    autil::MemUtil::markReadOnlyForDebug(block->data, _blockSize);
    return _cache->Insert(key, block, _blockSize, &DeleteBlock, handle, priority);
}

Block* MemoryBlockCache::Get(const blockid_t& blockId, CacheBase::Handle** handle) noexcept
{
    if (_memorySize == 0) {
        if (handle) {
            auto block = reinterpret_cast<Block*>(*handle);
            if (block) {
                return block;
            }
        }
        return nullptr;
    }
    assert(_cache);
    autil::StringView key(reinterpret_cast<const char*>(&blockId), sizeof(blockId));
    *handle = _cache->Lookup(key);
    if (*handle) {
        return reinterpret_cast<Block*>(_cache->Value(*handle));
    }

    return NULL;
}

void MemoryBlockCache::ReleaseHandle(CacheBase::Handle* handle) noexcept
{
    if (_memorySize == 0) {
        auto block = reinterpret_cast<Block*>(handle);
        if (block) {
            GetBlockAllocator()->FreeBlock(block);
        }
        return;
    }
    assert(_cache);
    if (handle) {
        _cache->Release(handle);
    }
}

uint32_t MemoryBlockCache::TEST_GetRefCount(CacheBase::Handle* handle)
{
    if (_memorySize == 0) {
        return 0;
    }
    if (strcmp(_cache->Name(), "LRUCache") == 0) {
        return reinterpret_cast<LRUHandle*>(handle)->refs;
    }
    assert(false);
    return 0;
}
}} // namespace indexlib::util
