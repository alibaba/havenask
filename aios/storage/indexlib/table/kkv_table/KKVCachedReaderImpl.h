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

#include <vector>

#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "autil/TimeoutTerminator.h"
#include "indexlib/index/kkv/building/KKVBuildingSegmentReader.h"
#include "indexlib/index/kkv/built/KKVBuiltSegmentReader.h"
#include "indexlib/index/kkv/common/KKVMetricsCollector.h"
#include "indexlib/index/kkv/common/Trait.h"
#include "indexlib/index/kkv/search/KKVCacheItem.h"
#include "indexlib/index/kkv/search/KKVIterator.h"
#include "indexlib/index/kkv/search/SearchCacheContext.h"
#include "indexlib/table/kkv_table/KKVIndexOptions.h"
#include "indexlib/table/kkv_table/KKVReadOptions.h"
#include "indexlib/table/kkv_table/KKVReaderImpl.h"
#include "indexlib/util/PooledUniquePtr.h"
#include "indexlib/util/ShardUtil.h"
#include "indexlib/util/Status.h"
#include "indexlib/util/cache/SearchCachePartitionWrapper.h"

namespace indexlibv2::table {

template <typename SKeyType>
class KKVCachedReaderImpl : public KKVReaderImpl<SKeyType>
{
public:
    using BuiltSegmentReaderTyped = index::KKVBuiltSegmentReader<SKeyType>;
    using BuiltSegmentReaderPtr = std::shared_ptr<BuiltSegmentReaderTyped>;
    using DiskShardReaderAndLocator = std::pair<BuiltSegmentReaderPtr, std::shared_ptr<framework::Locator>>;
    using DiskShardReaderVector = std::vector<std::vector<DiskShardReaderAndLocator>>;
    using BuildingSegmentReaderTyped = index::KKVBuildingSegmentReader<SKeyType>;
    using BuildingSegmentReaderPtr = std::shared_ptr<BuildingSegmentReaderTyped>;
    using MemShardReaderAndLocator = std::pair<BuildingSegmentReaderPtr, std::shared_ptr<framework::Locator>>;
    using MemShardReaderVector = std::vector<std::vector<MemShardReaderAndLocator>>;
    using SKeySet = std::unordered_set<SKeyType, std::hash<SKeyType>, std::equal_to<SKeyType>,
                                       autil::mem_pool::pool_allocator<SKeyType>>;

public:
    KKVCachedReaderImpl(schemaid_t readerSchemaId) : KKVReaderImpl<SKeyType>(readerSchemaId) {}

public:
    FL_LAZY(index::KKVIterator*)
    InnerLookup(index::PKeyType pkeyHash, const std::vector<uint64_t>& skeyHashVec, KKVReadOptions& readOptions,
                KKVIndexOptions& indexOptions) override;

    FL_LAZY(index::KKVIterator*)
    InnerLookup(index::PKeyType pkeyHash,
                const std::vector<uint64_t, autil::mem_pool::pool_allocator<uint64_t>>& skeyHashVec,
                KKVReadOptions& readOptions, KKVIndexOptions& indexOptions) override;

    void SetSearchCache(const indexlib::util::SearchCachePartitionWrapperPtr& cache) { _searchCache = cache; }

private:
    template <typename Alloc>
    FL_LAZY(index::KKVIterator*)
    DoInnerLookup(index::PKeyType pkeyHash, const std::vector<uint64_t, Alloc>& skeyHashVec,
                  KKVReadOptions& readOptions, KKVIndexOptions& indexOptions);

    template <typename Alloc>
    index::SearchContext<SKeyType>* PrepareContext(index::PKeyType pkeyHash,
                                                   const std::vector<uint64_t, Alloc>& skeyHashVec,
                                                   KKVReadOptions& readOptions, KKVIndexOptions& indexOptions);

    FL_LAZY(index::KKVIterator*) Search(index::SearchContext<SKeyType>& context);

    FL_LAZY(Status) GetKKVDocs(index::SearchContext<SKeyType>& context, index::KKVDocs& docs);

    FL_LAZY(Status) GetDocsFromBuilt(index::SearchContext<SKeyType>& context, index::KKVDocs& docs);

    void GetDocsFromCache(index::SearchContext<SKeyType>& context, index::KKVDocs& docs);

    void UpdateCache(index::SearchContext<SKeyType>& context, index::KKVDocs& docs, size_t builtBeginDocPos,
                     size_t builtEndDocPos);

    std::shared_ptr<framework::Locator> GetLastDiskSegmentLocator(size_t shardId) const;

private:
    indexlib::util::SearchCachePartitionWrapperPtr _searchCache;
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.table, KKVCachedReaderImpl, SKeyType);

///////////////////////////////////////////////////////////////////////

template <typename SKeyType>
inline FL_LAZY(index::KKVIterator*) KKVCachedReaderImpl<SKeyType>::InnerLookup(index::PKeyType pkeyHash,
                                                                               const std::vector<uint64_t>& skeyHashVec,
                                                                               KKVReadOptions& readOptions,
                                                                               KKVIndexOptions& indexOptions)
{
    FL_CORETURN FL_COAWAIT DoInnerLookup(pkeyHash, skeyHashVec, readOptions, indexOptions);
}

template <typename SKeyType>
inline FL_LAZY(index::KKVIterator*) KKVCachedReaderImpl<SKeyType>::InnerLookup(
    index::PKeyType pkeyHash, const std::vector<uint64_t, autil::mem_pool::pool_allocator<uint64_t>>& skeyHashVec,
    KKVReadOptions& readOptions, KKVIndexOptions& indexOptions)
{
    FL_CORETURN FL_COAWAIT DoInnerLookup(pkeyHash, skeyHashVec, readOptions, indexOptions);
}

template <typename SKeyType>
template <typename Alloc>
inline FL_LAZY(index::KKVIterator*) KKVCachedReaderImpl<SKeyType>::DoInnerLookup(
    index::PKeyType pkeyHash, const std::vector<uint64_t, Alloc>& skeyHashVec, KKVReadOptions& readOptions,
    KKVIndexOptions& indexOptions)
{
    if (readOptions.searchCacheType == indexlib::tsc_no_cache) {
        FL_CORETURN FL_COAWAIT KKVReaderImpl<SKeyType>::InnerLookup(pkeyHash, skeyHashVec, readOptions, indexOptions);
    }

    auto metricsCollector = readOptions.metricsCollector;
    if (!KKVReaderImpl<SKeyType>::_hasSegReader) {
        if (metricsCollector) {
            metricsCollector->EndQuery();
        }
        FL_CORETURN POOL_COMPATIBLE_NEW_CLASS(readOptions.pool, index::KKVIterator, readOptions.pool);
    }

    // TODO(xinfei.xsf) 这个unique_ptr使用有问题
    auto context = PrepareContext(pkeyHash, skeyHashVec, readOptions, indexOptions);
    indexlib::util::PooledUniquePtr<index::SearchContext<SKeyType>> contextPtr(context, readOptions.pool);
    FL_CORETURN FL_COAWAIT Search(*context);
}

template <typename SKeyType>
template <typename Alloc>
index::SearchContext<SKeyType>*
KKVCachedReaderImpl<SKeyType>::PrepareContext(index::PKeyType pkeyHash, const std::vector<uint64_t, Alloc>& skeyHashVec,
                                              KKVReadOptions& readOptions, KKVIndexOptions& indexOptions)
{
    size_t shardId = KKVReaderImpl<SKeyType>::GetShardId(pkeyHash);
    assert(shardId < KKVReaderImpl<SKeyType>::_diskShardReaders.size() &&
           shardId < KKVReaderImpl<SKeyType>::_memShardReaders.size());

    const auto& buildingSegReaders = KKVReaderImpl<SKeyType>::_memShardReaders[shardId];
    const auto& builtSegReaders = KKVReaderImpl<SKeyType>::_diskShardReaders[shardId];

    uint64_t currentTsInSecond = autil::TimeUtility::us2sec(readOptions.timestamp);
    auto context = IE_POOL_COMPATIBLE_NEW_CLASS(
        readOptions.pool, index::SearchContext<SKeyType>, readOptions.pool, indexOptions.GetIndexConfig().get(),
        pkeyHash, buildingSegReaders, builtSegReaders, currentTsInSecond, !indexOptions.GetSortParams().empty(),
        indexOptions.GetSKeyCountLimits(), readOptions.metricsCollector, _searchCache,
        readOptions.searchCacheType == indexlib::tsc_only_cache, indexOptions.GetPriority());
    context->shardId = shardId;
    // TODO(xinfei.sxf) add to construct function
    context->plainFormatEncoder = indexOptions.GetPlainFormatEncoder().get();

    if (currentTsInSecond > indexOptions.GetTTL() && !indexOptions.GetIndexConfig()->StoreExpireTime()) {
        context->minimumTsInSecond = currentTsInSecond - indexOptions.GetTTL();
    }
    if (!skeyHashVec.empty()) {
        auto skeyContext =
            IE_POOL_COMPATIBLE_NEW_CLASS(readOptions.pool, index::SKeySearchContext<SKeyType>, readOptions.pool);
        skeyContext->Init(skeyHashVec);
        context->skeyContext.reset(skeyContext, readOptions.pool);
        context->buildingFoundSKeys.reserve(skeyHashVec.size());
        context->builtFoundSKeys.reserve(skeyHashVec.size());
    }
    // TODO(xinfei.sxf) add metrics recoder

    return context;
}

template <typename SKeyType>
FL_LAZY(index::KKVIterator*)
KKVCachedReaderImpl<SKeyType>::Search(index::SearchContext<SKeyType>& context)
{
    index::KKVIterator* kkvIter = nullptr;
    index::KKVDocs kkvDocs(context.pool);
    auto status = FL_COAWAIT GetKKVDocs(context, kkvDocs);
    if (status.IsOK()) {
        auto bufferIter =
            POOL_COMPATIBLE_NEW_CLASS(context.pool, index::BufferedKKVIteratorImpl<SKeyType>, context.pool,
                                      context.pkey, std::move(kkvDocs), context.indexConfig, context.metricsCollector);
        kkvIter = POOL_COMPATIBLE_NEW_CLASS(context.pool, index::KKVIterator, context.pool, bufferIter,
                                            context.indexConfig, context.plainFormatEncoder, context.keepSortSeq,
                                            context.metricsCollector, context.skeyCountLimits);
    }
    FL_CORETURN kkvIter;
}

template <typename SKeyType>
FL_LAZY(Status)
KKVCachedReaderImpl<SKeyType>::GetKKVDocs(index::SearchContext<SKeyType>& context, index::KKVDocs& docs)
{
    KKVReaderImpl<SKeyType>::ResetCounter(context.metricsCollector);
    if (context.searchCacheContext.IsOnlyCache()) {
        if (context.metricsCollector) {
            context.metricsCollector->BeginSSTableQuery();
        }
        context.searchCacheContext.Init(context.pkey, context.skeyContext.get(), context.metricsCollector,
                                        context.pool);
        GetDocsFromCache(context, docs);
        FL_CORETURN Status::OK();
    }

    auto status = index::KKVSearchCoroutine<SKeyType>::SearchBuilding(context, docs);
    if (!status.IsOK()) {
        FL_CORETURN status;
    }
    context.buildingSKeyCount = docs.size();
    // TODO(xinfei.sxf) move terminate state to search context
    if (context.hasPKeyDeleted || context.currentSKeyCount >= context.skeyCountLimits) {
        FL_CORETURN Status::OK();
    }
    if (context.skeyContext && docs.size() == context.skeyContext->GetRequiredSKeyCount()) {
        FL_CORETURN Status::OK();
    }

    FL_CORETURN FL_COAWAIT GetDocsFromBuilt(context, docs);
}

template <typename SKeyType>
FL_LAZY(Status)
KKVCachedReaderImpl<SKeyType>::GetDocsFromBuilt(index::SearchContext<SKeyType>& context, index::KKVDocs& docs)
{
    if (context.builtSegReaders.empty()) {
        FL_CORETURN Status::OK();
    }

    if (context.metricsCollector) {
        context.metricsCollector->BeginSSTableQuery();
    }

    context.searchCacheContext.Init(context.pkey, context.skeyContext.get(), context.metricsCollector, context.pool);
    auto cacheItem = context.searchCacheContext.template GetCacheItem<SKeyType>();
    if (!cacheItem) {
        context.minLocator = nullptr;
    } else {
        context.minLocator = &cacheItem->locator;
    }

    auto status = FL_COAWAIT index::KKVSearchCoroutine<SKeyType>::SearchBuilt(context, docs);
    if (!status.IsOK()) {
        FL_CORETURN status;
    }
    // TODO(xinfei.sxf) don't account cache doc count in sstable count,
    // but v1 version do this, so compatible with v1 version
    if (!context.hasPKeyDeleted) {
        GetDocsFromCache(context, docs);
    }
    UpdateCache(context, docs, context.buildingSKeyCount, docs.size());

    FL_CORETURN Status::OK();
}

// TODO(xinfei.sxf) replace this function
inline bool SKeyExpired(const config::KVIndexConfig& kvConfig, uint64_t currentTsInSecond, uint64_t docTsInSecond,
                        uint64_t docExpireTime)
{
    if (!kvConfig.StoreExpireTime()) {
        return false;
    }
    if (docExpireTime == indexlib::UNINITIALIZED_EXPIRE_TIME) {
        int64_t configTTL = kvConfig.GetTTL();
        docExpireTime = docTsInSecond + configTTL;
    }
    return docExpireTime < currentTsInSecond;
}

template <typename SKeyType>
void KKVCachedReaderImpl<SKeyType>::GetDocsFromCache(index::SearchContext<SKeyType>& context, index::KKVDocs& docs)
{
    auto cacheItem = context.searchCacheContext.template GetCacheItem<SKeyType>();
    if (!cacheItem) {
        return;
    }

    auto skeyNodes = cacheItem->GetSKeyNodes();
    char* values = cacheItem->GetValues();
    uint32_t curValueBegin = 0;
    uint32_t count = 0;

    for (uint32_t i = 0; i < cacheItem->count; ++i) {
        // TODO(xinfei.sxf) deal currentBuiltSKeyCount in query built
        if (context.currentBuiltSKeyCount >= context.skeyCountLimits) {
            break;
        }

        index::KKVDoc doc;
        doc.inCache = true;
        doc.skey = skeyNodes[i].skey;
        doc.timestamp = skeyNodes[i].timestamp;
        doc.expireTime = skeyNodes[i].expireTime;
        auto curValueEnd = skeyNodes[i].valueOffset;
        auto curValueNonCopy = autil::StringView(values + curValueBegin, curValueEnd - curValueBegin);
        curValueBegin = curValueEnd;

        if (doc.timestamp < context.minimumTsInSecond ||
            SKeyExpired(*context.indexConfig, context.currentTsInSecond, doc.timestamp, doc.expireTime) ||
            context.builtFoundSKeys.find(skeyNodes[i].skey) != context.builtFoundSKeys.end()) {
            continue;
        }
        if (context.skeyContext && !context.skeyContext->MatchRequiredSKey(skeyNodes[i].skey)) {
            continue;
        }
        autil::StringView curValue = autil::MakeCString(curValueNonCopy, context.pool);
        doc.SetValue(curValue);

        // TODO(xinfei.sxf) this counter ignore of duplicatedKey, this is wrong
        ++context.currentBuiltSKeyCount;
        doc.duplicatedKey = (context.buildingFoundSKeys.find(skeyNodes[i].skey) != context.buildingFoundSKeys.end());
        if (!doc.duplicatedKey && ++context.currentSKeyCount > context.skeyCountLimits) {
            break;
        }
        docs.push_back(doc);
        if (!doc.duplicatedKey) { // NOTE(xinfei.sxf) count metric is not same with v1 version
            ++count;
            if (context.metricsCollector) {
                context.metricsCollector->IncResultCount();
            }
        }
    }
    if (context.metricsCollector) {
        context.metricsCollector->IncSearchCacheResultCount(count);
    }
}

template <typename SKeyType>
inline std::shared_ptr<framework::Locator>
KKVCachedReaderImpl<SKeyType>::GetLastDiskSegmentLocator(size_t shardId) const
{
    if (!KKVReaderImpl<SKeyType>::_diskShardReaders.empty() &&
        !KKVReaderImpl<SKeyType>::_diskShardReaders[shardId].empty()) {
        return KKVReaderImpl<SKeyType>::_diskShardReaders[shardId].rbegin()->second;
    }
    return nullptr;
}

template <typename SKeyType>
void KKVCachedReaderImpl<SKeyType>::UpdateCache(index::SearchContext<SKeyType>& context, index::KKVDocs& docs,
                                                size_t builtBeginDocPos, size_t builtEndDocPos)
{
    auto kkvCacheItem = context.searchCacheContext.template GetCacheItem<SKeyType>();
    // TODO(xinfei.sxf) fix needUpdateCache when item's locator is not valid
    bool needUpdateCache = !context.searchCacheContext.IsOnlyCache();

    bool isCacheValid = kkvCacheItem != nullptr;
    if (context.hasPKeyDeleted) {
        isCacheValid = false;
    }

    if (needUpdateCache) {
        if (!context.builtFoundSKeys.empty() || !isCacheValid) {
            context.updateCacheRatio = 100;
            auto docBeginIter = docs.begin() + builtBeginDocPos;
            auto docEndIter = docs.begin() + builtEndDocPos;

            if (context.plainFormatEncoder) {
                // make sure doc item in cache is decoded, so no need decode again when cache hit
                for (auto iter = docBeginIter; iter != docEndIter; ++iter) {
                    auto& doc = *iter;
                    if (doc.inCache || doc.skeyDeleted) {
                        continue;
                    }
                    auto ret = context.plainFormatEncoder->Decode(doc.value, context.pool, doc.value);
                    assert(ret);
                    (void)ret;
                    doc.inCache = true;
                }
            }
            index::KKVCacheItem<SKeyType>* newKkvCacheItem =
                index::KKVCacheItem<SKeyType>::Create(docBeginIter, docEndIter);

            auto locator = GetLastDiskSegmentLocator(context.shardId);
            if (locator) {
                newKkvCacheItem->locator = *locator;
            } else {
                assert(false);
                AUTIL_LOG(WARN, "disk segment locator must not be empty");
            }
            // TODO(xinfei.sxf) add metrics for insert cache
            context.searchCacheContext.PutCacheItem(newKkvCacheItem, context.searchCacheContext.GetPriority());
        } else {
            // TODO(xinfei.sxf) insert this item also to convert this item to used state
            auto locator = GetLastDiskSegmentLocator(context.shardId);
            if (locator) {
                kkvCacheItem->locator = *locator;
            } else {
                assert(false);
                AUTIL_LOG(WARN, "disk segment locator must not be empty");
            }
        }
    }
}

} // namespace indexlibv2::table
