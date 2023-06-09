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
#ifndef __INDEXLIB_KKV_SEARCH_COROUTINE_H
#define __INDEXLIB_KKV_SEARCH_COROUTINE_H

#include <memory>

#include "autil/ConstString.h"
#include "autil/MurmurHash.h"
#include "autil/NoCopyable.h"
#include "future_lite/MoveWrapper.h"
#include "indexlib/common_define.h"
#include "indexlib/index/kkv/building_kkv_segment_iterator.h"
#include "indexlib/index/kkv/building_kkv_segment_reader.h"
#include "indexlib/index/kkv/built_kkv_iterator.h"
#include "indexlib/index/kkv/built_kkv_segment_reader.h"
#include "indexlib/index/kkv/cached_kkv_iterator_impl.h"
#include "indexlib/index/kkv/kkv_coro_define.h"
#include "indexlib/index/kkv/kkv_define.h"
#include "indexlib/index/kkv/kkv_metrics_recorder.h"
#include "indexlib/index/kkv/search_cache_context.h"
#include "indexlib/index/kkv/skey_search_context.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/PooledUniquePtr.h"
#include "indexlib/util/cache/BlockCache.h"
#include "indexlib/util/cache/SearchCachePartitionWrapper.h"

namespace indexlib { namespace index {

#define RECORD_KKV_SEARCH_LATENCY(name)                                                                                \
    do {                                                                                                               \
        if (ctx.recorder) {                                                                                            \
            auto now = kmonitor_adapter::time::getCycles();                                                            \
            ctx.name = now - ctx.startSearchPoint;                                                                     \
        }                                                                                                              \
    } while (0)

template <typename SKeyType>
class KKVSearchCoroutine
{
private:
    enum Status {
        NORMAL,
        TERMINATE,
    };
    typedef std::pair<Status, KKVDoc> KKVResult;

    typedef BuildingKKVSegmentReader<SKeyType> BuildingSegReaderTyped;
    typedef std::shared_ptr<BuildingSegReaderTyped> BuildingSegReaderTypedPtr;
    typedef BuiltKKVSegmentReader<SKeyType> BuiltSegReaderTyped;
    DEFINE_SHARED_PTR(BuiltSegReaderTyped);

    typedef BuildingKKVSegmentIterator<SKeyType> BuildingKKVSegIter;

    using SKeySet = std::unordered_set<SKeyType, std::hash<SKeyType>, std::equal_to<SKeyType>,
                                       autil::mem_pool::pool_allocator<SKeyType>>;

    struct SegResult : private autil::NoCopyable {
        KKVDocs kkvDocs;
        bool hasPKeyDeleted = false;
        uint32_t pKeyDeletedTs = 0;
        KKVValueFetcher valueFetcher;
        bool lastSeg = false;
        SegResult(autil::mem_pool::Pool* pool) : kkvDocs(pool) {}
        SegResult(SegResult&& other)
            : kkvDocs(std::move(other.kkvDocs))
            , hasPKeyDeleted(other.hasPKeyDeleted)
            , pKeyDeletedTs(other.pKeyDeletedTs)
            , valueFetcher(std::move(other.valueFetcher))
            , lastSeg(other.lastSeg)
        {
        }
    };
    using SegResultTry = future_lite::Try<SegResult>;
    using SKeyUnorderedSet = std::unordered_set<SKeyType, std::hash<SKeyType>, std::equal_to<SKeyType>,
                                                autil::mem_pool::pool_allocator<SKeyType>>;

public:
    template <typename Alloc>
    static FL_LAZY(CachedKKVIteratorImpl<SKeyType>*)
        Search(autil::mem_pool::Pool* pool, KKVIndexOptions* indexOptions, PKeyType pKey,
               std::vector<uint64_t, Alloc> suffixKeyHashVec,
               const std::vector<BuiltSegReaderTypedPtr>& builtSegReaders,
               const std::vector<BuildingSegReaderTypedPtr>& buildingSegReaders,
               const util::SearchCachePartitionWrapperPtr& searchCache, uint64_t currentTimeInSecond, bool onlyCache,
               KVMetricsCollector* metricsCollector, KKVMetricsRecorder* recorder);

    static FL_LAZY(bool)
        CollectDocsFromBuiltSegment(SearchContext<SKeyType>& ctx, SKeySet& builtFoundSKeys, segmentid_t minSegmentId,
                                    KKVDocs& kkvDocs, autil::mem_pool::Pool* pool);

private:
    template <typename Iter>
    static bool HasPKeyDeletedByTime(Iter* iter, uint64_t tsThresholdInSecond)
    {
        if (!iter->HasPKeyDeleted()) {
            return false;
        }
        auto deleteTs = iter->GetPKeyDeletedTs();
        return deleteTs >= tsThresholdInSecond;
    }

    //  @return is terminated
    static bool SearchBuilding(SearchContext<SKeyType>& ctx, KKVDocs& kkvDocs, autil::mem_pool::Pool* pool);

    static FL_LAZY(CachedKKVIteratorImpl<SKeyType>*)
        SearchBuilt(util::PooledUniquePtr<SearchContext<SKeyType>> ctx, KKVDocs kkvDocs, autil::mem_pool::Pool* pool);

    static void CollectDocsFromCache(SearchContext<SKeyType>& ctx,
                                     KKVSearchCoroutine<SKeyType>::SKeySet& builtFoundSKeys, KKVDocs& docs,
                                     autil::mem_pool::Pool* pool);

    static FL_LAZY(std::pair<bool, std::vector<SegResult, autil::mem_pool::pool_allocator<SegResult>>>)
        CollectSKeysFromBuiltSegments(SearchContext<SKeyType>& ctx, segmentid_t minSegmentId,
                                      autil::mem_pool::Pool* pool);

    template <typename SegIter>
    static bool IsLastSegment(const SearchContext<SKeyType>& ctx, bool hasPKeyDeleted, SegIter curIter, SegIter endIter)
    {
        if (ctx.searchCacheCtx.HasCacheItem()) {
            return false;
        }
        if (hasPKeyDeleted) {
            return true;
        }
        if (curIter + 1 == endIter) {
            return true;
        }
        return false;
    }

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, KKVSearchCoroutine);

template <typename SKeyType>
template <typename Alloc>
inline FL_LAZY(CachedKKVIteratorImpl<SKeyType>*) KKVSearchCoroutine<SKeyType>::Search(
    autil::mem_pool::Pool* pool, KKVIndexOptions* indexOptions, PKeyType pKey,
    std::vector<uint64_t, Alloc> suffixKeyHashVec, const std::vector<BuiltSegReaderTypedPtr>& builtSegReaders,
    const std::vector<BuildingSegReaderTypedPtr>& buildingSegReaders,
    const util::SearchCachePartitionWrapperPtr& searchCache, uint64_t currentTimeInSecond, bool onlyCache,
    KVMetricsCollector* metricsCollector, KKVMetricsRecorder* recorder)
{
    assert(indexOptions);

    //  why not allocate SearchContext on stack?
    auto searchCtx = IE_POOL_COMPATIBLE_NEW_CLASS(pool, SearchContext<SKeyType>, buildingSegReaders, builtSegReaders,
                                                  searchCache, onlyCache, pool);
    util::PooledUniquePtr<SearchContext<SKeyType>> ctxPointer(searchCtx, pool);
    auto& ctx = *ctxPointer;
    ctx.pKey = pKey;
    ctx.indexOptions = indexOptions;

    if (!suffixKeyHashVec.empty()) {
        auto skeyCtx = IE_POOL_COMPATIBLE_NEW_CLASS(pool, SKeySearchContext<SKeyType>, pool);
        ctx.skeyCtx.reset(skeyCtx, pool);
        ctx.skeyCtx->Init(suffixKeyHashVec);
    }
    ctx.minimumTsInSecond = 0;
    ctx.currentTsInSecond = currentTimeInSecond;
    if (currentTimeInSecond > indexOptions->ttl && !indexOptions->kvConfig->StoreExpireTime()) {
        ctx.minimumTsInSecond = currentTimeInSecond - indexOptions->ttl;
    }
    ctx.metricsCollector = metricsCollector;
    if (recorder) {
        ctx.recorder = recorder;
        ctx.startSearchPoint = kmonitor_adapter::time::getCycles();
    }

    if (ctx.metricsCollector) {
        ctx.metricsCollector->ResetBlockCounter();
        ctx.metricsCollector->BeginMemTableQuery();
    }

    if (ctx.searchCacheCtx.GetSearchCache().operator bool()) {
        ctx.searchCacheCtx.Init(ctx.pKey, ctx.indexOptions, ctx.skeyCtx.get(), ctx.metricsCollector, pool);
    }

    KKVDocs kkvDocs(pool);
    ctx.incTs = ctx.indexOptions->incTsInSecond;
    if (!ctx.searchCacheCtx.IsOnlyCache()) {
        auto cacheItem = ctx.searchCacheCtx.template GetCacheItem<SKeyType>();
        if (cacheItem) {
            ctx.incTs = cacheItem->timestamp;
        }
        RECORD_KKV_SEARCH_LATENCY(beforeSearchBuildingLatency);
        auto terminated = SearchBuilding(ctx, kkvDocs, pool);
        if (terminated) {
            FL_CORETURN FL_COAWAIT[](
                autil::mem_pool::Pool * pool, PKeyType pKey, KKVDocs kkvDocs, KKVIndexOptions * indexOptions,
                KVMetricsCollector * metricsCollector) mutable->FL_LAZY(CachedKKVIteratorImpl<SKeyType>*)
            {
                FL_CORETURN IE_POOL_COMPATIBLE_NEW_CLASS(pool, CachedKKVIteratorImpl<SKeyType>, pool, pKey,
                                                         std::move(kkvDocs), indexOptions, metricsCollector);
            }
            (pool, pKey, std::move(kkvDocs), indexOptions, metricsCollector);
        }
    }
    RECORD_KKV_SEARCH_LATENCY(beforeSearchBuiltLatency);

    auto ret = FL_COAWAIT SearchBuilt(std::move(ctxPointer), std::move(kkvDocs), pool);
    RECORD_KKV_SEARCH_LATENCY(searchKKVLatency);
    ctx.RecordSearch();
    FL_CORETURN std::move(ret);
}

template <typename SKeyType>
inline bool KKVSearchCoroutine<SKeyType>::SearchBuilding(SearchContext<SKeyType>& ctx, KKVDocs& docs,
                                                         autil::mem_pool::Pool* pool)
{
    auto skeyCountLimits = ctx.indexOptions->GetSKeyCountLimits();
    for (auto& segReader : ctx.buildingSegReaders) {
        if (ctx.skeyCtx) {
            ctx.skeyCtx->ResetSKeySKipInfo();
        }
        auto buildingSegIter =
            segReader->template Lookup<BuildingKKVSegmentIterator<SKeyType>>(ctx.pKey, pool, ctx.indexOptions);
        if (!buildingSegIter) {
            continue;
        }
        util::PooledUniquePtr<KKVSegmentIterator> iterHolder(buildingSegIter, pool);
        if (ctx.skeyCtx) {
            ctx.skeyCtx->UpdateNeedSeekBySKey(buildingSegIter->HasSkipList());
        }

        for (;;) {
            KKVDoc singleDoc;
            SKeyType sk = {};
            bool hasValidSKey =
                buildingSegIter->MoveToValidPosition(ctx.skeyCtx.get(), std::max(ctx.minimumTsInSecond, ctx.incTs),
                                                     ctx.currentTsInSecond, ctx.foundSkeys, sk, singleDoc.timestamp);
            singleDoc.skey = sk;

            // if (needSeekBySKey)
            // {
            //     hasValidSKey = buildingSegIter->MoveToValidSKey(
            //         std::max(ctx.minimumTsInSecond, ctx.incTs), ctx.sortedRequiredSkeys,
            //         ctx.foundSkeys, currentSeekSKeyPos, skey, singleDoc.timestamp);
            //     ++currentSeekSKeyPos;
            // }
            // else
            // {
            //     hasValidSKey = buildingSegIter->MoveToValidPosition(
            //         std::max(ctx.minimumTsInSecond, ctx.incTs), ctx.maxRequiredSkey,
            //         ctx.requiredSkeys, ctx.foundSkeys, skey, singleDoc.timestamp);
            // }

            ++ctx.seekSKeyCount;
            if (hasValidSKey) {
                if (ctx.metricsCollector) {
                    ctx.metricsCollector->IncResultCount();
                }
                if (++ctx.currentSKeyCount > skeyCountLimits) {
                    return true;
                }
                buildingSegIter->GetCurrentValue(singleDoc.value);
                docs.push_back(singleDoc);
            } else if (HasPKeyDeletedByTime(buildingSegIter, ctx.incTs)) {
                if (ctx.metricsCollector) {
                    ctx.metricsCollector->IncResultCount();
                }
                return true;
            } else {
                break; // next reader
            }
        }
    }
    return false;
}

template <typename SKeyType>
inline FL_LAZY(CachedKKVIteratorImpl<SKeyType>*) KKVSearchCoroutine<SKeyType>::SearchBuilt(
    util::PooledUniquePtr<SearchContext<SKeyType>> ctxPointer, KKVDocs docs, autil::mem_pool::Pool* pool)
{
    assert(ctxPointer);
    auto searchCtx = std::move(ctxPointer);
    auto& ctx = *searchCtx;
    auto builtBeginDocPos = docs.size();
    if (ctx.metricsCollector) {
        ctx.metricsCollector->BeginSSTableQuery();
    }
    segmentid_t minSegmentId = INVALID_SEGMENTID;
    bool isCacheValid = false;
    auto kkvCacheItem = ctx.searchCacheCtx.template GetCacheItem<SKeyType>();

    if (kkvCacheItem) {
        assert(kkvCacheItem->nextRtSegmentId >= ctx.indexOptions->oldestRtSegmentId);
        isCacheValid = true;
        minSegmentId = kkvCacheItem->nextRtSegmentId;
    }

    bool needUpdateCache = !ctx.searchCacheCtx.IsOnlyCache() && minSegmentId < ctx.indexOptions->buildingSegmentId;
    autil::mem_pool::pool_allocator<SKeyType> alloc(pool);
    SKeySet builtFoundSKeys(alloc); // Cache need all skeys, use discrete set.
    if (needUpdateCache) {
        auto hasPKeyDeleted = FL_COAWAIT CollectDocsFromBuiltSegment(ctx, builtFoundSKeys, minSegmentId, docs, pool);
        if (hasPKeyDeleted) {
            isCacheValid = false;
        }
    }
    RECORD_KKV_SEARCH_LATENCY(beforeFetchCacheLatency);
    if (isCacheValid) {
        CollectDocsFromCache(ctx, builtFoundSKeys, docs, pool);
    }
    size_t builtEndDocPos = docs.size();
    if (needUpdateCache && ctx.searchCacheCtx.GetSearchCache().operator bool()) {
        if (!builtFoundSKeys.empty() || !isCacheValid) {
            ctx.updateCacheRatio = 100;
            auto docBeginIter = docs.begin() + builtBeginDocPos;
            auto docEndIter = docs.begin() + builtEndDocPos;

            if (ctx.indexOptions->plainFormatEncoder) {
                // make sure doc item in cache is decoded, so no need decode again when cache hit
                for (auto iter = docBeginIter; iter != docEndIter; ++iter) {
                    auto& doc = *iter;
                    if (doc.inCache || doc.skeyDeleted) {
                        continue;
                    }
                    auto ret = ctx.indexOptions->plainFormatEncoder->Decode(doc.value, pool, doc.value);
                    assert(ret);
                    (void)ret;
                    doc.inCache = true;
                }
            }
            KKVCacheItem<SKeyType>* newKkvCacheItem = KKVCacheItem<SKeyType>::Create(docBeginIter, docEndIter);
            newKkvCacheItem->nextRtSegmentId = ctx.indexOptions->buildingSegmentId;
            newKkvCacheItem->timestamp = ctx.incTs;
            ctx.searchCacheCtx.PutCacheItem(ctx.indexOptions->GetRegionId(), newKkvCacheItem,
                                            ctx.indexOptions->cachePriority);
        } else {
            // new result is same as result in cache
            kkvCacheItem->nextRtSegmentId = ctx.indexOptions->buildingSegmentId;
        }
    }
    FL_CORO_TRACE("kkv[%lu] finish search built", ctx.pKey);
    FL_CORETURN IE_POOL_COMPATIBLE_NEW_CLASS(pool, CachedKKVIteratorImpl<SKeyType>, pool, ctx.pKey, std::move(docs),
                                             ctx.indexOptions, ctx.metricsCollector);
}

template <typename SKeyType>
inline void KKVSearchCoroutine<SKeyType>::CollectDocsFromCache(SearchContext<SKeyType>& ctx,
                                                               KKVSearchCoroutine<SKeyType>::SKeySet& builtFoundSKeys,
                                                               KKVDocs& docs, autil::mem_pool::Pool* pool)
{
    auto cacheItem = ctx.searchCacheCtx.template GetCacheItem<SKeyType>();
    if (!cacheItem) {
        return;
    }
    CachedSKeyNode<SKeyType>* skeyNodes = cacheItem->GetSKeyNodes();
    char* values = cacheItem->GetValues();
    uint32_t curValueBegin = 0;
    auto skeyCountLimits = ctx.indexOptions->GetSKeyCountLimits();
    uint32_t count = 0;
    for (uint32_t i = 0; i < cacheItem->count; ++i) {
        if (ctx.currentBuiltSKeyCount >= skeyCountLimits) {
            break;
        }

        KKVDoc doc;
        doc.inCache = true;
        doc.skey = skeyNodes[i].skey;
        doc.timestamp = skeyNodes[i].timestamp;
        doc.expireTime = skeyNodes[i].expireTime;
        uint32_t curValueEnd = skeyNodes[i].valueOffset;
        autil::StringView curValueNonCopy = autil::StringView(values + curValueBegin, curValueEnd - curValueBegin);
        curValueBegin = curValueEnd;
        if (doc.timestamp < ctx.minimumTsInSecond ||
            SKeyExpired(*ctx.indexOptions->kvConfig, ctx.currentTsInSecond, doc.timestamp, doc.expireTime) ||
            builtFoundSKeys.find(skeyNodes[i].skey) != builtFoundSKeys.end()) {
            continue;
        }
        if (ctx.skeyCtx && !ctx.skeyCtx->MatchRequiredSKey(skeyNodes[i].skey)) {
            continue;
        }
        autil::StringView curValue = autil::MakeCString(curValueNonCopy, pool);
        doc.SetValue(curValue);

        ++count;
        ++ctx.currentBuiltSKeyCount;
        doc.duplicatedKey = (ctx.foundSkeys.find(skeyNodes[i].skey) != ctx.foundSkeys.end());
        if (!doc.duplicatedKey && ++ctx.currentSKeyCount > skeyCountLimits) {
            break;
        }
        docs.push_back(doc);
        if (ctx.metricsCollector) {
            ctx.metricsCollector->IncResultCount();
        }
    }
    if (ctx.metricsCollector) {
        ctx.metricsCollector->IncSearchCacheResultCount(count);
    }
}

template <typename SKeyType>
inline FL_LAZY(bool) KKVSearchCoroutine<SKeyType>::CollectDocsFromBuiltSegment(
    SearchContext<SKeyType>& ctx, KKVSearchCoroutine<SKeyType>::SKeySet& builtFoundSKeys, segmentid_t minSegmentId,
    KKVDocs& kkvDocs, autil::mem_pool::Pool* pool)
{
    FL_CORO_TRACE("kkv[%lu] start search built segment", ctx.pKey);
    auto ret = FL_COAWAIT CollectSKeysFromBuiltSegments(ctx, minSegmentId, pool);
    bool builtHasPKeyDeleted = ret.first;
    auto segResults = std::move(ret.second);

    RECORD_KKV_SEARCH_LATENCY(beforeFetchValueLatency);

    bool keepSortSeq = !ctx.indexOptions->sortParams.empty();
    bool terminated = false;
    auto skeyCountLimits = ctx.indexOptions->GetSKeyCountLimits();
    autil::mem_pool::pool_allocator<FL_LAZY(future_lite::Unit)> alloc(pool);
    std::vector<FL_LAZY(future_lite::Unit), autil::mem_pool::pool_allocator<FL_LAZY(future_lite::Unit)>>
        fetchValueTasks(alloc);
    for (auto iter = segResults.begin(); iter != segResults.end() && !terminated; ++iter) {
        auto& result = *iter;
        auto& segDocs = result.kkvDocs;
        auto beginDocPos = kkvDocs.size();
        for (auto& doc : segDocs) {
            // TODO: use ValidateSKey()
            SKeyType curSKey = doc.skey;
            if (ctx.skeyCtx) {
                if (ctx.skeyCtx->MatchAllRequiredSKeys(builtFoundSKeys.size())) {
                    terminated = true;
                    break; // done
                }
                if (!keepSortSeq && curSKey > ctx.skeyCtx->GetMaxRequiredSKey()) {
                    break; // next segment
                }
            }
            bool found = false;
            if (result.lastSeg && !ctx.skeyCtx) {
                found = builtFoundSKeys.find(curSKey) != builtFoundSKeys.end();
            } else {
                found = !(builtFoundSKeys.insert(curSKey).second);
            }
            if (found || doc.skeyDeleted ||
                SKeyExpired(*ctx.indexOptions->kvConfig, ctx.currentTsInSecond, doc.timestamp, doc.expireTime)) {
                continue; // next doc
            }
            ++ctx.currentBuiltSKeyCount;
            doc.duplicatedKey = ctx.foundSkeys.find(curSKey) != ctx.foundSkeys.end();
            if (!doc.duplicatedKey && ++ctx.currentSKeyCount > skeyCountLimits) {
                terminated = true;
                break;
            }
            kkvDocs.push_back(doc);
            if (!doc.duplicatedKey && !doc.skeyDeleted && ctx.metricsCollector) {
                ctx.metricsCollector->IncResultCount();
            }
        }
        if (result.valueFetcher) {
            auto fetchValueTask = [](size_t beginDocPos, size_t endDocPos, KKVValueFetcher fetcher,
                                     KKVDocs& kkvDocs) mutable -> FL_LAZY(future_lite::Unit) {
                FL_COAWAIT fetcher.FetchValues(kkvDocs.begin() + beginDocPos, kkvDocs.begin() + endDocPos);
                FL_CORETURN future_lite::Unit {};
            }(beginDocPos, kkvDocs.size(), std::move(result.valueFetcher), kkvDocs);
            fetchValueTasks.push_back(std::move(fetchValueTask));
        }
    }
    autil::mem_pool::pool_allocator<future_lite::Try<future_lite::Unit>> outAlloc(pool);
    auto out = FL_COAWAIT future_lite::interface::collectAll(std::move(fetchValueTasks), outAlloc);
    for (auto& _ : out) {
        auto ret = future_lite::interface::getTryValue(_); // rethrow exception if error
        (void)ret;
    }
    FL_CORETURN builtHasPKeyDeleted;
}

template <typename SKeyType>
inline FL_LAZY(
    std::pair<bool, std::vector<typename KKVSearchCoroutine<SKeyType>::SegResult,
                                autil::mem_pool::pool_allocator<typename KKVSearchCoroutine<SKeyType>::SegResult>>>)
    KKVSearchCoroutine<SKeyType>::CollectSKeysFromBuiltSegments(SearchContext<SKeyType>& ctx, segmentid_t minSegmentId,
                                                                autil::mem_pool::Pool* pool)
{
    autil::mem_pool::pool_allocator<FL_LAZY(SegResult)> alloc(pool);
    std::vector<FL_LAZY(SegResult), autil::mem_pool::pool_allocator<FL_LAZY(SegResult)>> segTasks(alloc);
    bool builtHasPKeyDeleted = false;
    auto endIter = ctx.builtSegReaders.rend();
    for (size_t i = 0; i < ctx.builtSegReaders.size(); ++i) {
        if (ctx.builtSegReaders[i]->GetTimestampInSecond() >= ctx.minimumTsInSecond) {
            endIter = ctx.builtSegReaders.rbegin() + (ctx.builtSegReaders.size() - i);
            break;
        }
    }
    for (auto iter = ctx.builtSegReaders.rbegin(); iter != endIter; ++iter) {
        auto& segReader = *iter;
        if (segReader->GetSegmentId() >= minSegmentId && segReader->GetTimestampInSecond() >= ctx.minimumTsInSecond) {
            SegResult segResult(pool);
            auto rawDocIter = FL_COAWAIT segReader->template Lookup<KKVBuiltSegmentDocIterator<SKeyType>>(
                ctx.pKey, pool, ctx.indexOptions, ctx.metricsCollector);
            if (!rawDocIter) {
                continue;
            }
            util::PooledUniquePtr<KKVBuiltSegmentDocIterator<SKeyType>> docIter;
            docIter.reset(rawDocIter, pool);
            segResult.valueFetcher = std::move(docIter->StealValueFetcher());
            if (true == (segResult.hasPKeyDeleted = docIter->HasPKeyDeleted())) {
                segResult.pKeyDeletedTs = docIter->GetPKeyDeletedTs();
            }
            uint64_t minimumTsInSecond = ctx.minimumTsInSecond;
            uint64_t incTsThreshold = 0;
            ++ctx.seekSegmentCount;
            if (segReader->IsRealtimeSegment()) {
                ++ctx.seekRtSegmentCount;
                incTsThreshold = ctx.incTs;
                minimumTsInSecond = std::max(minimumTsInSecond, ctx.incTs);
            }
            bool hasPKeyDeleted = segResult.hasPKeyDeleted && segResult.pKeyDeletedTs >= incTsThreshold;
            segResult.lastSeg = IsLastSegment(ctx, hasPKeyDeleted, iter, endIter);
            auto segTask = [](util::PooledUniquePtr<KKVBuiltSegmentDocIterator<SKeyType>> docIter, SegResult segResult,
                              SearchContext<SKeyType>& ctx, uint64_t minimumTsInSecond) mutable -> FL_LAZY(SegResult) {
                while (docIter->IsValid()) {
                    ++ctx.seekSKeyCount;
                    auto doc = docIter->GetCurrentDoc();

                    if (doc.timestamp >= minimumTsInSecond &&
                        (!ctx.skeyCtx || ctx.skeyCtx->MatchRequiredSKey(doc.skey))) {
                        segResult.kkvDocs.push_back(doc);
                    }

                    if (!docIter->MoveToNext()) {
                        if (docIter->HasHitLastNode()) {
                            break;
                        }
                        FL_COAWAIT docIter->SwitchChunk();
                    }
                }
                FL_CORETURN std::move(segResult);
            }(std::move(docIter), std::move(segResult), ctx, minimumTsInSecond);
            segTasks.push_back(std::move(segTask));
            if (hasPKeyDeleted) {
                builtHasPKeyDeleted = true;
                break;
            }
        }
    }
    // TODO: serialize search on pkey+skey search
    autil::mem_pool::pool_allocator<future_lite::Try<SegResult>> outAlloc(pool);
    auto tryRet = FL_COAWAIT future_lite::interface::collectAll(std::move(segTasks), outAlloc);
    autil::mem_pool::pool_allocator<SegResult> retAlloc(pool);
    std::vector<SegResult, autil::mem_pool::pool_allocator<SegResult>> ret(retAlloc);
    for (auto& it : tryRet) {
        ret.push_back(std::move(future_lite::interface::getTryValue(it)));
    }

    FL_CORETURN make_pair(builtHasPKeyDeleted, std::move(ret));
}

#undef RECORD_KKV_SEARCH_LATENCY
}} // namespace indexlib::index

#endif //__INDEXLIB_KKV_SEARCH_COROUTINE_H
