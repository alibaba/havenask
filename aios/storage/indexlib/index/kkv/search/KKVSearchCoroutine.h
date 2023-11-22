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

#include "autil/ConstString.h"
#include "autil/Log.h"
#include "autil/MurmurHash.h"
#include "autil/NoCopyable.h"
#include "future_lite/CoroInterface.h"
#include "future_lite/MoveWrapper.h"
#include "indexlib/framework/Locator.h"
#include "indexlib/index/kkv/building/KKVBuildingSegmentReader.h"
#include "indexlib/index/kkv/built/KKVBuiltSegmentReader.h"
#include "indexlib/index/kkv/built/KKVBuiltValueFetcher.h"
#include "indexlib/index/kkv/search/BufferedKKVIteratorImpl.h"
#include "indexlib/index/kkv/search/KKVMetricsRecorder.h"
#include "indexlib/index/kkv/search/SearchContext.h"
#include "indexlib/util/PooledUniquePtr.h"
#include "indexlib/util/cache/BlockCache.h"
#include "indexlib/util/cache/SearchCachePartitionWrapper.h"

namespace indexlibv2::index {

#define RECORD_KKV_SEARCH_LATENCY(name)                                                                                \
    do {                                                                                                               \
        if (context.recorder) {                                                                                        \
            auto now = kmonitor_adapter::time::getCycles();                                                            \
            context.name = now - context.startSearchPoint;                                                             \
        }                                                                                                              \
    } while (0)

#define COLLECT_METRICS(metricsCollector, func)                                                                        \
    if (metricsCollector) {                                                                                            \
        metricsCollector->func();                                                                                      \
    }
template <typename SKeyType>
class KKVSearchCoroutine
{
private:
    struct SegResult : private autil::NoCopyable {
        KKVDocs kkvDocs;
        bool hasPKeyDeleted = false;
        indexlib::util::PooledUniquePtr<KKVBuiltSegmentIteratorBase<SKeyType>> iterHolder;
        KKVBuiltValueFetcher valueFetcher;
        bool lastSeg = false;
        SegResult(autil::mem_pool::Pool* pool) : kkvDocs(pool) {}
        SegResult(SegResult&& other)
            : kkvDocs(std::move(other.kkvDocs))
            , hasPKeyDeleted(other.hasPKeyDeleted)
            , iterHolder(std::move(other.iterHolder))
            , valueFetcher(std::move(other.valueFetcher))
            , lastSeg(other.lastSeg)
        {
        }
    };

private:
    using BuildingSegmentReaderTyped = KKVBuildingSegmentReader<SKeyType>;
    using BuildingSegmentReaderPtr = std::shared_ptr<BuildingSegmentReaderTyped>;
    using MemShardReaderAndLocator = std::pair<BuildingSegmentReaderPtr, std::shared_ptr<framework::Locator>>;
    using BuildingSegmentIterator = KKVBuildingSegmentIterator<SKeyType>;
    using BuiltSegmentReaderTyped = KKVBuiltSegmentReader<SKeyType>;
    using BuiltSegmentReaderPtr = std::shared_ptr<BuiltSegmentReaderTyped>;
    using DiskShardReaderAndLocator = std::pair<BuiltSegmentReaderPtr, std::shared_ptr<framework::Locator>>;
    using BuiltSegmentIterator = KKVBuiltSegmentIteratorBase<SKeyType>;
    using SKeyContext = SKeySearchContext<SKeyType>;
    using SKeySet = typename SKeyContext::SKeySet;
    using PooledSKeySet = typename SKeyContext::PooledSKeySet;

    using SegResultVec = std::vector<SegResult, autil::mem_pool::pool_allocator<SegResult>>;
    using SegResultTry = future_lite::Try<SegResult>;

public:
    static Status SearchBuilding(SearchContext<SKeyType>& context, KKVDocs& kkvDocs);
    static FL_LAZY(Status) SearchBuilt(SearchContext<SKeyType>& context, KKVDocs& kkvDocs);

public:
    // for now, corotuine&cache always bind, so this func only use for test
    template <typename Alloc>
    static FL_LAZY(std::pair<Status, BufferedKKVIteratorImpl<SKeyType>*>)
        Search(autil::mem_pool::Pool* pool, config::KKVIndexConfig* indexConfig, uint64_t ttl, bool keepSortSequence,
               uint64_t skeyCountLimits, PKeyType pkey, const std::vector<uint64_t, Alloc>& skeyHashVec,
               const std::vector<DiskShardReaderAndLocator>& builtSegReaders,
               const std::vector<MemShardReaderAndLocator>& buildingSegReaders, uint64_t currentTimeInSecond,
               KKVMetricsCollector* metricsCollector);

private:
    static FL_LAZY(Status) CollectSKeysFromBuiltSegments(SearchContext<SKeyType>& context, SegResultVec& segResultVec);
    static bool IsLocatorObsolete(framework::Locator* minLocator, framework::Locator* currentLocator);

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(index, KKVSearchCoroutine, SKeyType);

template <typename SKeyType>
inline Status KKVSearchCoroutine<SKeyType>::SearchBuilding(SearchContext<SKeyType>& context, KKVDocs& result)
{
    assert(result.size() == 0); // search building first and only once

    auto skeyContext = context.skeyContext.get();
    auto pool = context.pool;
    auto skeyCountLimits = context.skeyCountLimits;
    auto metricsCollector = context.metricsCollector;
    auto& buildingFoundSKeys = context.buildingFoundSKeys;

    KKVResultBuffer resultBuffer(skeyCountLimits, skeyCountLimits, pool);
    for (auto& segReader : context.buildingSegReaders) {
        auto buildingSegIter = segReader.first->Lookup(context.pkey, pool, metricsCollector);
        if (!buildingSegIter) {
            continue;
        }
        using IterType = KKVBuildingSegmentIterator<SKeyType>;
        indexlib::util::PooledUniquePtr<IterType> iterHolder(buildingSegIter, pool);
        // TODO(xinfei.sxf) has found all skey -> break;
        buildingSegIter->BatchGet(skeyContext, context.minimumTsInSecond, context.currentTsInSecond, buildingFoundSKeys,
                                  resultBuffer);
        if (buildingSegIter->HasPKeyDeleted()) {
            context.hasPKeyDeleted = true;
            ++context.seekSKeyCount;
            break;
        }
    }
    resultBuffer.Swap(result);

    context.seekSKeyCount += result.size();
    context.currentSKeyCount += result.size();
    if (metricsCollector) {
        metricsCollector->IncResultCount(result.size());
    }
    return Status::OK();
}

template <typename SKeyType>
inline FL_LAZY(Status) KKVSearchCoroutine<SKeyType>::SearchBuilt(SearchContext<SKeyType>& context, KKVDocs& result)
{
    FL_CORO_TRACE("kkv[%lu] start search built segment", context.pkey);

    auto pool = context.pool;
    autil::mem_pool::pool_allocator<SegResult> segResultAlloc(pool);
    SegResultVec segResults(segResultAlloc);

    auto status = FL_COAWAIT CollectSKeysFromBuiltSegments(context, segResults);
    if (!status.IsOK()) {
        FL_CORETURN status;
    }

    RECORD_KKV_SEARCH_LATENCY(beforeFetchValueLatency);

    bool keepSortSeq = context.keepSortSeq;
    auto skeyCountLimits = context.skeyCountLimits;
    bool terminated = false;

    // coroutine can use void but pool_allocator not
    using TaskType = FL_LAZY(bool);
    autil::mem_pool::pool_allocator<TaskType> alloc(pool);
    std::vector<TaskType, autil::mem_pool::pool_allocator<TaskType>> fetchValueTasks(alloc);

    auto& builtFoundSKeys = context.builtFoundSKeys;
    auto& buildingFoundSKeys = context.buildingFoundSKeys;
    auto skeyContext = context.skeyContext.get();
    auto indexConfig = context.indexConfig;
    bool storeExpireTime = indexConfig->StoreExpireTime();
    auto defaultTTL = indexConfig->GetTTL();
    auto currentTsInSecond = context.currentTsInSecond;

    auto segResultIter = segResults.begin();
    for (; segResultIter != segResults.end() && !terminated; ++segResultIter) {
        auto& segResult = *segResultIter;
        auto& segDocs = segResult.kkvDocs;
        auto beginDocPos = result.size();
        context.seekSKeyCount += segDocs.size();
        for (auto& doc : segDocs) {
            SKeyType curSKey = doc.skey;
            if (skeyContext) {
                if (skeyContext->MatchAllRequiredSKeys(builtFoundSKeys.size())) {
                    terminated = true;
                    break; // done
                }
                if (!keepSortSeq && curSKey > skeyContext->GetMaxRequiredSKey()) {
                    break; // next segment
                }
            }
            bool found = false;
            if (segResult.lastSeg && !skeyContext) {
                found = builtFoundSKeys.find(curSKey) != builtFoundSKeys.end();
            } else {
                found = !(builtFoundSKeys.insert(curSKey).second);
            }
            if (found || doc.skeyDeleted ||
                (storeExpireTime &&
                 KKVTTLHelper::SKeyExpired(currentTsInSecond, doc.timestamp, doc.expireTime, defaultTTL))) {
                continue; // next doc
            }
            // TODO(qisa.cb) may don't need duplicatedKey flag anymore
            doc.duplicatedKey = buildingFoundSKeys.find(curSKey) != buildingFoundSKeys.end();
            result.push_back(doc);
            if (!doc.duplicatedKey) {
                ++context.currentSKeyCount;
                COLLECT_METRICS(context.metricsCollector, IncResultCount);
            }
            if (context.currentSKeyCount >= skeyCountLimits) {
                terminated = true;
                break;
            }
        }
        if (segResult.valueFetcher) {
            auto fetchValueTask = segResult.valueFetcher.FetchValues(result.begin() + beginDocPos, result.end());
            fetchValueTasks.push_back(std::move(fetchValueTask));
        }
    }

    // make sure the metrics right
    while (segResultIter != segResults.end()) {
        context.seekSKeyCount += segResultIter->kkvDocs.size();
        ++segResultIter;
    }

    autil::mem_pool::pool_allocator<future_lite::Try<bool>> outAlloc(pool);
    auto allResult = FL_COAWAIT future_lite::interface::collectAll(std::move(fetchValueTasks), outAlloc);
    for (const auto& oneResult : allResult) {
        // NOTE, not expected exception in under layer function
        auto ret = future_lite::interface::getTryValue(oneResult); // rethrow exception if error
        if (!ret) {
            FL_CORETURN Status::IOError();
        }
    }

    FL_CORETURN Status::OK();
}

template <typename SKeyType>
inline FL_LAZY(Status) KKVSearchCoroutine<SKeyType>::CollectSKeysFromBuiltSegments(
    SearchContext<SKeyType>& context, typename KKVSearchCoroutine<SKeyType>::SegResultVec& segResultVec)
{
    auto pool = context.pool;
    using TaskType = FL_LAZY(std::pair<Status, SegResult>);
    autil::mem_pool::pool_allocator<TaskType> alloc(pool);
    std::vector<TaskType, autil::mem_pool::pool_allocator<TaskType>> segTasks(alloc);

    auto& builtSegReaders = context.builtSegReaders;
    auto endIter = builtSegReaders.rend();

    for (size_t i = 0; i < builtSegReaders.size(); ++i) {
        if (builtSegReaders[i].first->GetTimestampInSecond() >= context.minimumTsInSecond) {
            endIter = builtSegReaders.rbegin() + (builtSegReaders.size() - i);
            break;
        }
    }

    auto skeyContext = context.skeyContext.get();
    PKeyType pkey = context.pkey;
    auto metricsCollector = context.metricsCollector;
    auto minLocator = context.minLocator;
    auto minimumTsInSecond = context.minimumTsInSecond;
    Status retStatus = Status::OK();

    for (auto iter = builtSegReaders.rbegin(); iter != endIter; ++iter) {
        auto segReader = iter->first.get();
        if (segReader->GetTimestampInSecond() >= minimumTsInSecond) {
            // for no need seek old segment case, eg cache
            if (minLocator && IsLocatorObsolete(minLocator, iter->second.get())) {
                continue;
            }
            SegResult segResult(pool);
            // TODO(xinfei.sxf) 这里造成了串行化扫描多个segment,每个segment甚至会扫描2个chunk。
            auto [status, docIter] = FL_COAWAIT segReader->Lookup(pkey, pool, metricsCollector);
            if (!status.IsOK()) {
                retStatus = status;
                break;
            }
            if (!docIter) {
                continue;
            }
            segResult.iterHolder.reset(docIter, pool);

            segResult.hasPKeyDeleted = docIter->HasPKeyDeleted();
            bool hasPKeyDeleted = segResult.hasPKeyDeleted;
            uint64_t minimumTsInSecond = context.minimumTsInSecond;
            ++context.seekSegmentCount;
            if (segReader->IsRealtimeSegment()) {
                ++context.seekRtSegmentCount;
            }
            segResult.lastSeg = (minLocator == nullptr) && (hasPKeyDeleted || (iter + 1 == endIter));

            // TODO(xinfei.sxf) don't return segResult
            auto segTask = [](KKVBuiltSegmentIteratorBase<SKeyType>* docIter, SegResult segResult,
                              SKeySearchContext<SKeyType>* skeyContext,
                              uint64_t minimumTsInSecond) mutable -> TaskType {
                auto ret = FL_COAWAIT docIter->GetSKeysAsync(skeyContext, minimumTsInSecond, segResult.kkvDocs,
                                                             segResult.valueFetcher);
                if (!ret) {
                    FL_CORETURN std::make_pair(Status::IOError(), std::move(segResult));
                } else {
                    FL_CORETURN std::make_pair(Status::OK(), std::move(segResult));
                }
            }(std::move(docIter), std::move(segResult), skeyContext, minimumTsInSecond);

            segTasks.push_back(std::move(segTask));
            if (hasPKeyDeleted) {
                ++context.seekSKeyCount;
                context.hasPKeyDeleted = true;
                break;
            }
        }
    }

    if (retStatus.IsOK()) {
        autil::mem_pool::pool_allocator<future_lite::Try<std::pair<Status, SegResult>>> outAlloc(pool);
        // NOTE, not expected exception in under layer function
        auto tryRet = FL_COAWAIT future_lite::interface::collectAll(std::move(segTasks), outAlloc);
        for (auto& it : tryRet) {
            // TODO(xinfei.sxf) is this exist copy
            auto& [status, segResult] = future_lite::interface::getTryValue(it); // rethrow exception if error
            if (!status.IsOK()) {
                FL_CORETURN status;
            }
            segResultVec.push_back(std::move(segResult));
        }
    }

    FL_CORETURN retStatus;
}

template <typename SKeyType>
bool KKVSearchCoroutine<SKeyType>::IsLocatorObsolete(framework::Locator* minLocator, framework::Locator* currentLocator)
{
    auto ret = minLocator->IsFasterThan(*currentLocator, true);
    assert(ret != framework::Locator::LocatorCompareResult::LCR_INVALID);
    return ret == framework::Locator::LocatorCompareResult::LCR_FULLY_FASTER;
}

template <typename SKeyType>
template <typename Alloc>
FL_LAZY(std::pair<Status, BufferedKKVIteratorImpl<SKeyType>*>)
KKVSearchCoroutine<SKeyType>::Search(autil::mem_pool::Pool* pool, config::KKVIndexConfig* indexConfig, uint64_t ttl,
                                     bool keepSortSeq, uint64_t skeyCountLimits, PKeyType pkey,
                                     const std::vector<uint64_t, Alloc>& skeyHashVec,
                                     const std::vector<DiskShardReaderAndLocator>& builtSegReaders,
                                     const std::vector<MemShardReaderAndLocator>& buildingSegReaders,
                                     uint64_t currentTsInSecond, KKVMetricsCollector* metricsCollector)
{
    SearchContext context(pool, indexConfig, pkey, buildingSegReaders, builtSegReaders, currentTsInSecond, keepSortSeq,
                          skeyCountLimits, metricsCollector, nullptr, true, autil::CacheBase::Priority::HIGH);

    if (currentTsInSecond > ttl && !indexConfig->StoreExpireTime()) {
        context.minimumTsInSecond = currentTsInSecond - ttl;
    }
    if (!skeyHashVec.empty()) {
        auto skeyContext = IE_POOL_COMPATIBLE_NEW_CLASS(pool, SKeyContext, pool);
        skeyContext->Init(skeyHashVec);
        context.skeyContext.reset(skeyContext, pool);
        context.buildingFoundSKeys.reserve(skeyHashVec.size());
        context.builtFoundSKeys.reserve(skeyHashVec.size());
    }
    if (metricsCollector != nullptr) {
        metricsCollector->BeginMemTableQuery();
        metricsCollector->ResetBlockCounter();
    }

    RECORD_KKV_SEARCH_LATENCY(beforeSearchBuildingLatency);
    KKVDocs kkvDocs(pool);
    auto status = SearchBuilding(context, kkvDocs);
    if (!status.IsOK()) {
        FL_CORETURN std::make_pair(status, nullptr);
    }
    if (!context.hasPKeyDeleted && context.currentSKeyCount < skeyCountLimits) {
        RECORD_KKV_SEARCH_LATENCY(beforeSearchBuiltLatency);
        status = FL_COAWAIT SearchBuilt(context, kkvDocs);
        if (!status.IsOK()) {
            FL_CORETURN std::make_pair(status, nullptr);
        }
    }
    auto bufferedKKVIterator = POOL_COMPATIBLE_NEW_CLASS(pool, BufferedKKVIteratorImpl<SKeyType>, pool, pkey,
                                                         std::move(kkvDocs), indexConfig, metricsCollector);
    RECORD_KKV_SEARCH_LATENCY(searchKKVLatency);
    context.RecordSearch();
    FL_CORETURN std::make_pair(status, bufferedKKVIterator);
}

#undef COLLECT_METRICS
#undef RECORD_KKV_SEARCH_LATENCY
} // namespace indexlibv2::index
