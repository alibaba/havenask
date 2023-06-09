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

#include "autil/Log.h"
#include "indexlib/util/cache/BlockCache.h"

namespace indexlib { namespace util {

class MemoryBlockCache : public BlockCache
{
public:
    MemoryBlockCache();
    ~MemoryBlockCache();

    MemoryBlockCache(const MemoryBlockCache&) = delete;
    MemoryBlockCache& operator=(const MemoryBlockCache&) = delete;
    MemoryBlockCache(MemoryBlockCache&&) = delete;
    MemoryBlockCache& operator=(MemoryBlockCache&&) = delete;

public:
    bool DoInit(const BlockCacheOption& cacheOption) override;

    bool Put(Block* block, autil::CacheBase::Handle** handle, autil::CacheBase::Priority priority) noexcept override;

    Block* Get(const blockid_t& blockId, autil::CacheBase::Handle** handle) noexcept override;
    void ReleaseHandle(autil::CacheBase::Handle* handle) noexcept override;

    CacheResourceInfo GetResourceInfo() const noexcept override
    {
        CacheResourceInfo info;
        info.maxMemoryUse = _memorySize;
        info.memoryUse = _cache ? _cache->GetUsage() : 0;
        info.maxDiskUse = 0;
        info.diskUse = 0;
        return info;
    }
    uint32_t GetBlockCount() const override { return _cache ? (_cache->GetUsage() / _blockSize) : 0; }
    uint32_t GetMaxBlockCount() const override { return _cache ? (_cache->GetCapacity() / _blockSize) : 0; }

    const char* TEST_GetCacheName() const override { return _cache ? _cache->Name() : "unknown"; }
    std::shared_ptr<autil::CacheBase> TEST_GetCache() const { return _cache; }
    uint32_t TEST_GetRefCount(autil::CacheBase::Handle* handle) override;

private:
    std::shared_ptr<autil::CacheBase> _cache;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<MemoryBlockCache> MemoryBlockCachePtr;
}} // namespace indexlib::util
