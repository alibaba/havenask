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

#include <unordered_set>

#include "autil/cache/cache.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/pool_allocator.h"
#include "indexlib/framework/Locator.h"
#include "indexlib/index/common/field_format/pack_attribute/PlainFormatEncoder.h"
#include "indexlib/index/kkv/common/KKVMetricsCollector.h"
#include "indexlib/index/kkv/common/SKeySearchContext.h"
#include "indexlib/index/kkv/search/KKVMetricsRecorder.h"
#include "indexlib/index/kkv/search/SearchCacheContext.h"
#include "indexlib/util/PooledUniquePtr.h"
#include "indexlib/util/cache/BlockCache.h"
#include "indexlib/util/cache/SearchCachePartitionWrapper.h"

namespace indexlibv2::index {

template <typename SKeyType>
class KKVBuildingSegmentReader;
template <typename SKeyType>
class KKVBuiltSegmentReader;

template <typename SKeyType>
struct SearchContext {
private:
    using BuildingSegmentReaderTyped = KKVBuildingSegmentReader<SKeyType>;
    using BuildingSegmentReaderPtr = std::shared_ptr<BuildingSegmentReaderTyped>;
    using MemShardReaderAndLocator = std::pair<BuildingSegmentReaderPtr, std::shared_ptr<framework::Locator>>;
    using BuiltSegmentReaderTyped = KKVBuiltSegmentReader<SKeyType>;
    using BuiltSegmentReaderPtr = std::shared_ptr<BuiltSegmentReaderTyped>;
    using DiskShardReaderAndLocator = std::pair<BuiltSegmentReaderPtr, std::shared_ptr<framework::Locator>>;
    using SKeyContext = SKeySearchContext<SKeyType>;
    using PooledSKeySet = typename SKeyContext::PooledSKeySet;

public:
    SearchContext(const SearchContext<SKeyType>&) = delete;
    SearchContext& operator=(const SearchContext<SKeyType>&) = delete;

public:
    autil::mem_pool::Pool* pool = nullptr;
    config::KKVIndexConfig* indexConfig = nullptr;
    PKeyType pkey;
    size_t shardId;
    const std::vector<MemShardReaderAndLocator>& buildingSegReaders;
    const std::vector<DiskShardReaderAndLocator>& builtSegReaders;
    uint64_t currentTsInSecond = 0;
    bool keepSortSeq = false;
    uint64_t skeyCountLimits = 0;
    KKVMetricsCollector* metricsCollector = nullptr;
    PooledSKeySet buildingFoundSKeys;
    PooledSKeySet builtFoundSKeys;
    SearchCacheContext searchCacheContext;

    // for no need seek old segment case, eg cache
    framework::Locator* minLocator = nullptr;
    uint64_t minimumTsInSecond = 0;
    indexlib::util::PooledUniquePtr<SKeySearchContext<SKeyType>> skeyContext;

    bool hasPKeyDeleted = false;

    // counter for kkv docs
    uint32_t currentSKeyCount = 0; // not include duplicatedKey
    uint32_t buildingSKeyCount = 0;
    uint32_t builtSKeyCount = 0;
    uint32_t cachedSKeyCount = 0;
    uint32_t currentBuiltSKeyCount = 0;

    index::PlainFormatEncoder* plainFormatEncoder = nullptr;

public:
    // recorders
    KKVMetricsRecorder* recorder = nullptr;
    uint64_t startSearchPoint = 0;
    uint32_t beforeSearchBuildingLatency = 0;
    uint32_t beforeSearchBuiltLatency = 0;
    uint32_t searchKKVLatency = 0;
    uint32_t beforeFetchValueLatency = 0;
    uint32_t beforeFetchCacheLatency = 0;
    uint16_t updateCacheRatio = 0;
    uint16_t seekRtSegmentCount = 0; // not include building
    uint16_t seekSegmentCount = 0;   // not include building
    uint32_t seekSKeyCount = 0;      // doc count return from iterator

public:
    SearchContext(autil::mem_pool::Pool* pool, config::KKVIndexConfig* indexConfig, PKeyType pkey,
                  const std::vector<MemShardReaderAndLocator>& buildingSegReaders,
                  const std::vector<DiskShardReaderAndLocator>& builtSegReaders, uint64_t currentTsInSecond,
                  bool keepSortSeq, uint64_t skeyCountLimits, KKVMetricsCollector* metricsCollector)
        : SearchContext(pool, indexConfig, pkey, buildingSegReaders, builtSegReaders, currentTsInSecond, keepSortSeq,
                        skeyCountLimits, metricsCollector, nullptr, false, autil::CacheBase::Priority::HIGH)
    {
    }

    SearchContext(autil::mem_pool::Pool* pool, config::KKVIndexConfig* indexConfig, PKeyType pkey,
                  const std::vector<MemShardReaderAndLocator>& buildingSegReaders,
                  const std::vector<DiskShardReaderAndLocator>& builtSegReaders, uint64_t currentTsInSecond,
                  bool keepSortSeq, uint64_t skeyCountLimits, KKVMetricsCollector* metricsCollector,
                  const indexlib::util::SearchCachePartitionWrapperPtr& searchCache, bool onlyCache,
                  autil::CacheBase::Priority cachePriority)
        : pool(pool)
        , indexConfig(indexConfig)
        , pkey(pkey)
        , buildingSegReaders(buildingSegReaders)
        , builtSegReaders(builtSegReaders)
        , currentTsInSecond(currentTsInSecond)
        , keepSortSeq(keepSortSeq)
        , skeyCountLimits(skeyCountLimits)
        , metricsCollector(metricsCollector)
        , buildingFoundSKeys(autil::mem_pool::pool_allocator<SKeyType>(pool))
        , builtFoundSKeys(autil::mem_pool::pool_allocator<SKeyType>(pool))
        , searchCacheContext(searchCache, onlyCache, cachePriority)
    {
    }

    void RecordSearch()
    {
        if (recorder) {
            auto* group = recorder->GetThreadGroup();
            assert(group);
            group->beforeSearchBuildingLatency.record(beforeSearchBuildingLatency);
            group->beforeSearchBuiltLatency.record(beforeSearchBuiltLatency);
            group->searchKKVLatency.record(searchKKVLatency);
            group->beforeFetchValueLatency.record(beforeFetchValueLatency);
            group->beforeFetchCacheLatency.record(beforeFetchCacheLatency);
            group->updateCacheRatio.record(updateCacheRatio);
            group->seekRtSegmentCount.record(seekRtSegmentCount);
            group->seekSegmentCount.record(seekSegmentCount);
            group->seekSKeyCount.record(seekSKeyCount);
            group->matchedSKeyCount.record(currentSKeyCount);
        }
    }
};

} // namespace indexlibv2::index
