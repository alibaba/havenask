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
#ifndef __INDEXLIB_KKV_CORO_DEFINE_H
#define __INDEXLIB_KKV_CORO_DEFINE_H

#include <unordered_set>

#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/pool_allocator.h"
#include "future_lite/CoroInterface.h"
#include "indexlib/index/kkv/cached_kkv_iterator_impl.h"
#include "indexlib/index/kkv/kkv_define.h"
#include "indexlib/index/kkv/kkv_index_options.h"
#include "indexlib/index/kkv/kkv_metrics_recorder.h"
#include "indexlib/index/kkv/search_cache_context.h"
#include "indexlib/index/kkv/skey_search_context.h"
#include "indexlib/index/kv/kv_metrics_collector.h"
#include "indexlib/index_define.h"
#include "indexlib/util/PooledUniquePtr.h"
#include "indexlib/util/cache/BlockCache.h"
#include "indexlib/util/cache/SearchCachePartitionWrapper.h"

namespace indexlib { namespace index {

template <typename SKeyType>
class BuildingKKVSegmentReader;
template <typename SKeyType>
class BuiltKKVSegmentReader;

template <typename SKeyType>
struct SearchContext {
private:
    typedef BuildingKKVSegmentReader<SKeyType> BuildingSegReaderTyped;
    typedef std::shared_ptr<BuildingSegReaderTyped> BuildingSegReaderTypedPtr;
    typedef BuiltKKVSegmentReader<SKeyType> BuiltSegReaderTyped;
    DEFINE_SHARED_PTR(BuiltSegReaderTyped);
    typedef std::unordered_set<SKeyType, std::hash<SKeyType>, std::equal_to<SKeyType>,
                               autil::mem_pool::pool_allocator<SKeyType>>
        SKeyUnorderedSet;

public:
    SearchContext(const SearchContext<SKeyType>&) = delete;
    SearchContext& operator=(const SearchContext<SKeyType>&) = delete;

public:
    const std::vector<BuildingSegReaderTypedPtr>& buildingSegReaders;
    const std::vector<BuiltSegReaderTypedPtr>& builtSegReaders;
    SearchCacheContext searchCacheCtx;

    PKeyType pKey = 0;
    KKVIndexOptions* indexOptions = nullptr;
    util::PooledUniquePtr<SKeySearchContext<SKeyType>> skeyCtx;

    uint64_t minimumTsInSecond = 0;
    uint64_t currentTsInSecond = 0;
    KVMetricsCollector* metricsCollector = nullptr;

    // recorders
    KKVMetricsRecorder* recorder = nullptr;
    uint64_t startSearchPoint = 0;
    uint32_t beforeSearchBuildingLatency = 0;
    uint32_t beforeSearchBuiltLatency = 0;
    uint32_t searchKKVLatency = 0;
    uint32_t beforeFetchValueLatency = 0;
    uint32_t beforeFetchCacheLatency = 0;
    uint16_t updateCacheRatio = 0;
    uint16_t seekRtSegmentCount = 0;
    uint16_t seekSegmentCount = 0;
    uint32_t seekSKeyCount = 0;

    SKeyUnorderedSet foundSkeys;
    uint32_t currentSKeyCount = 0;
    uint32_t currentBuiltSKeyCount = 0;
    uint64_t incTs = 0;

    SearchContext(const std::vector<BuildingSegReaderTypedPtr>& buildingSegReaders,
                  const std::vector<BuiltSegReaderTypedPtr>& builtSegReaders,
                  const util::SearchCachePartitionWrapperPtr& searchCache, bool onlyCache, autil::mem_pool::Pool* pool)
        : buildingSegReaders(buildingSegReaders)
        , builtSegReaders(builtSegReaders)
        , searchCacheCtx(searchCache, onlyCache)
        , foundSkeys(autil::mem_pool::pool_allocator<SKeyType>(pool))
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
}} // namespace indexlib::index

#endif //__INDEXLIB_KKV_DEFINE_H
