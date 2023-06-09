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
#ifndef __INDEXLIB_KV_READER_IMPL_H
#define __INDEXLIB_KV_READER_IMPL_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileBlockCache.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/index/kv/kv_cache_item.h"
#include "indexlib/index/kv/kv_index_options.h"
#include "indexlib/index/kv/kv_reader.h"
#include "indexlib/index/kv/kv_segment_reader.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/online_segment_directory.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/ColumnUtil.h"
#include "indexlib/util/cache/BlockCache.h"
#include "indexlib/util/cache/SearchCachePartitionWrapper.h"

namespace indexlib { namespace index {

enum KVResultStatus {
    NOT_FOUND,
    FOUND_INVALID_KEY,
    FOUND_VALID_KEY,
};

class KVReaderImpl final : public KVReader
{
public:
    KVReaderImpl()
    {
        mHasTTL = false;
        mHasBuildingSegment = false;
        mHasOnlineSegment = true;
        mHasMultiRegion = false;
        mHasSearchCache = false;
        mKVReportMetrics = true;
        mBuildingSegmentId = INVALID_SEGMENTID;
    }

    ~KVReaderImpl() {}

private:
    typedef KVSegmentReader OfflineSegmentReader;
    typedef KVSegmentReader OnlineSegmentReader;

public:
    void Open(const config::KVIndexConfigPtr& kvConfig, const index_base::PartitionDataPtr& partitionData,
              const util::SearchCachePartitionWrapperPtr& searchCache, int64_t latestIncSkipTs) override;
    KVReaderPtr CreateCodegenKVReader() override;
    KVReader* Clone() override;
    void TEST_collectCodegenResult(CodegenCheckers& checkers, std::string id) override;

private:
    // int64_t GetSearchHitCount() const { return mHasSearchCache ? mSearchCache->GetHitCount() : 0; }
    // int64_t GetSearchMissCount() const { return mHasSearchCache ? mSearchCache->GetMissCount() : 0; }

private:
    void LoadSegmentReaders(const index_base::PartitionDataPtr& partitionData,
                            const config::KVIndexConfigPtr& kvIndexConfig);

    void LoadSegmentReaders(const index_base::PartitionDataPtr& partitionData,
                            const config::KVIndexConfigPtr& kvIndexConfig, const index_base::Version& version,
                            std::vector<std::vector<OfflineSegmentReader>>& segmentReaders);
    void LoadOnlineSegmentReaders(const index_base::PartitionDataPtr& partitionData,
                                  const config::KVIndexConfigPtr& kvIndexConfig, const index_base::Version& version,
                                  std::vector<OnlineSegmentReader>& segmentReaders);

    FL_LAZY(bool)
    GetImplWithoutCache(const KVIndexOptions* options, keytype_t literalKey, keytype_t key,
                        TableSearchCacheType searchCacheType, autil::StringView& value, uint64_t& valueTs,
                        autil::mem_pool::Pool* pool, KVMetricsCollector* metricsCollector) const;

    FL_LAZY(bool)
    GetImpl(const KVIndexOptions* options, keytype_t literalKey, keytype_t key, TableSearchCacheType searchCacheType,
            autil::StringView& value, uint64_t& valueTs, autil::mem_pool::Pool* pool,
            KVMetricsCollector* metricsCollector) const;

    void ResetCounter(KVMetricsCollector* metricsCollector) const;

private:
    bool GetRegionId(const KVIndexOptions* options) const;
    uint64_t GetLookupKeyHash(uint64_t key, const KVIndexOptions* options) const;
    FL_LAZY(KVResultStatus)
    GetFromSegmentReaders(const KVIndexOptions* options,
                          const std::vector<std::vector<OfflineSegmentReader>>& segmentReaders, keytype_t literalKey,
                          keytype_t key, autil::StringView& value, uint64_t& valueTs, autil::mem_pool::Pool* pool,
                          KVMetricsCollector* metricsCollector) const;

    FL_LAZY(KVResultStatus)
    GetFromOnlineSegments(const KVIndexOptions* options, keytype_t literalKey, keytype_t key, autil::StringView& value,
                          uint64_t& valueTs, autil::mem_pool::Pool* pool,
                          KVMetricsCollector* metricsCollector) const __ALWAYS_INLINE;

    FL_LAZY(KVResultStatus)
    GetFromOfflineSegments(const KVIndexOptions* options, keytype_t literalKey, keytype_t key, autil::StringView& value,
                           uint64_t& valueTs, autil::mem_pool::Pool* pool,
                           KVMetricsCollector* metricsCollector) const __ALWAYS_INLINE;

    FL_LAZY(KVResultStatus)
    GetFromBuildingSegment(const KVIndexOptions* options, keytype_t literalKey, keytype_t key, autil::StringView& value,
                           uint64_t& valueTs, autil::mem_pool::Pool* pool, KVMetricsCollector* metricsCollector) const;

    FL_LAZY(KVResultStatus)
    GetFromRtSegments(const KVIndexOptions* options, segmentid_t nextRtSegmentId, keytype_t literalKey, keytype_t key,
                      autil::StringView& value, uint64_t& valueTs, autil::mem_pool::Pool* pool,
                      KVMetricsCollector* metricsCollector) const;

    void PutCache(keytype_t key, regionid_t regionId, const autil::StringView& value, segmentid_t nextRtSegmentId,
                  uint32_t timestamp, bool hasValue, autil::CacheBase::Priority priority) const;

protected:
    FL_LAZY(bool)
    InnerGet(const KVIndexOptions* options, keytype_t key, autil::StringView& value, uint64_t ts,
             TableSearchCacheType searchCacheType, autil::mem_pool::Pool* pool = NULL,
             KVMetricsCollector* metricsCollector = NULL) const override;
    FL_LAZY(bool)
    InnerGet(const KVIndexOptions* options, const autil::StringView& key, autil::StringView& value, uint64_t ts,
             TableSearchCacheType searchCacheType, autil::mem_pool::Pool* pool = NULL,
             KVMetricsCollector* metricsCollector = NULL) const override;

    bool doCollectAllCode() override;

private:
    std::vector<std::vector<OfflineSegmentReader>> mOfflineSegmentReaders;
    std::vector<OnlineSegmentReader> mOnlineSegmentReaders;
    util::SearchCachePartitionWrapperPtr mSearchCache;
    segmentid_t mBuildingSegmentId;

    // todo codegen to const
    bool mHasTTL;
    bool mHasBuildingSegment;
    bool mHasOnlineSegment;
    bool mHasMultiRegion;
    bool mHasSearchCache;
    bool mKVReportMetrics;

private:
    friend class KVReaderImplTest;
    IE_LOG_DECLARE();
};
DEFINE_SHARED_PTR(KVReaderImpl);

///////////////////////////////////////////////////////////////////////
inline void KVReaderImpl::ResetCounter(KVMetricsCollector* metricsCollector) const
{
    if (!mKVReportMetrics || !metricsCollector) {
        return;
    }
    metricsCollector->BeginMemTableQuery();
    if (mHasSearchCache) {
        // mSearchCache->ResetCounter();
    }
    metricsCollector->ResetBlockCounter();
}

inline uint64_t KVReaderImpl::GetLookupKeyHash(uint64_t key, const KVIndexOptions* options) const
{
    if (!mHasMultiRegion) {
        return key;
    }
    return options->GetLookupKeyHash(key);
}

inline FL_LAZY(bool) KVReaderImpl::InnerGet(const KVIndexOptions* options, keytype_t key, autil::StringView& value,
                                            uint64_t ts, TableSearchCacheType searchCacheType,
                                            autil::mem_pool::Pool* pool, KVMetricsCollector* metricsCollector) const
{
    assert(options);
    ResetCounter(metricsCollector);

    keytype_t searchKey = GetLookupKeyHash(key, options);
    uint64_t valueTs = 0;
    bool ret = FL_COAWAIT GetImpl(options, key, searchKey, searchCacheType, value, valueTs, pool, metricsCollector);
    if (mHasTTL && ret) {
        uint64_t minimumTsInSecond = 0;
        uint64_t currentTimeInSecond = MicrosecondToSecond(ts);
        if (currentTimeInSecond > options->ttl) {
            minimumTsInSecond = currentTimeInSecond - options->ttl;
        }
        ret = valueTs >= minimumTsInSecond;
    }
    if (mKVReportMetrics && metricsCollector) {
        metricsCollector->EndQuery();
        // metricsCollector->IncSearchCacheHitCount(GetSearchHitCount());
        // metricsCollector->IncSearchCacheMissCount(GetSearchMissCount());
    }
    FL_CORETURN ret;
}

inline FL_LAZY(bool) KVReaderImpl::InnerGet(const KVIndexOptions* options, const autil::StringView& key,
                                            autil::StringView& value, uint64_t ts, TableSearchCacheType searchCacheType,
                                            autil::mem_pool::Pool* pool, KVMetricsCollector* metricsCollector) const
{
    keytype_t hashKey;
    if (likely(GetHashKey(key, hashKey))) {
        FL_CORETURN FL_COAWAIT InnerGet(options, hashKey, value, ts, searchCacheType, pool, metricsCollector);
    }
    if (mKVReportMetrics && metricsCollector) {
        metricsCollector->EndQuery();
    }
    FL_CORETURN false;
}

inline FL_LAZY(bool) KVReaderImpl::GetImplWithoutCache(const KVIndexOptions* options, keytype_t literalKey,
                                                       keytype_t key, TableSearchCacheType searchCacheType,
                                                       autil::StringView& value, uint64_t& valueTs,
                                                       autil::mem_pool::Pool* pool,
                                                       KVMetricsCollector* metricsCollector) const
{
    KVResultStatus ret =
        FL_COAWAIT GetFromOnlineSegments(options, literalKey, key, value, valueTs, pool, metricsCollector);

    if (mKVReportMetrics && metricsCollector) {
        metricsCollector->BeginSSTableQuery();
    }
    if (ret == NOT_FOUND || valueTs < options->incTsInSecond) {
        ret = FL_COAWAIT GetFromOfflineSegments(options, literalKey, key, value, valueTs, pool, metricsCollector);
    }
    FL_CORETURN ret == FOUND_VALID_KEY;
}

inline bool KVReaderImpl::GetRegionId(const KVIndexOptions* options) const
{
    if (!mHasMultiRegion) {
        return DEFAULT_REGIONID;
    }
    return options->GetRegionId();
}

inline FL_LAZY(bool) KVReaderImpl::GetImpl(const KVIndexOptions* options, keytype_t literalKey, keytype_t key,
                                           TableSearchCacheType searchCacheType, autil::StringView& value,
                                           uint64_t& valueTs, autil::mem_pool::Pool* pool,
                                           KVMetricsCollector* metricsCollector) const
{
    if (!mHasSearchCache || searchCacheType == tsc_no_cache) {
        FL_CORETURN FL_COAWAIT GetImplWithoutCache(options, literalKey, key, searchCacheType, value, valueTs, pool,
                                                   metricsCollector);
    }

    segmentid_t oldestRtSegmentId = options->oldestRtSegmentId;
    if (searchCacheType == tsc_only_cache) {
        util::SearchCacheCounter* searchCacheCounter = nullptr;
        if (mKVReportMetrics && metricsCollector) {
            metricsCollector->BeginSSTableQuery();
            searchCacheCounter = metricsCollector->GetSearchCacheCounter();
        }

        std::unique_ptr<util::CacheItemGuard> cacheItemGuard;
        KVCacheItem* kvCacheItem = NULL;
        if (mHasSearchCache &&
            (cacheItemGuard = mSearchCache->Get((uint64_t)key, GetRegionId(options), searchCacheCounter)) &&
            (kvCacheItem = cacheItemGuard->GetCacheItem<KVCacheItem>()) &&
            (kvCacheItem->nextRtSegmentId >= oldestRtSegmentId)) {
            assert(pool);
            value = autil::MakeCString(kvCacheItem->value, pool);
            valueTs = kvCacheItem->timestamp;
            if (mKVReportMetrics && metricsCollector && kvCacheItem->HasValue()) {
                metricsCollector->IncSearchCacheResultCount(1);
                metricsCollector->IncResultCount();
            }
            FL_CORETURN kvCacheItem->HasValue();
        }
        FL_CORETURN false;
    }
    KVResultStatus ret =
        FL_COAWAIT GetFromBuildingSegment(options, literalKey, key, value, valueTs, pool, metricsCollector);
    bool hitBuilding = (ret != NOT_FOUND);
    if (hitBuilding && valueTs >= options->incTsInSecond) {
        FL_CORETURN ret == FOUND_VALID_KEY;
    }

    util::SearchCacheCounter* searchCacheCounter = nullptr;
    if (mKVReportMetrics && metricsCollector) {
        metricsCollector->BeginSSTableQuery();
        searchCacheCounter = metricsCollector->GetSearchCacheCounter();
    }

    std::unique_ptr<util::CacheItemGuard> cacheItemGuard;
    KVCacheItem* kvCacheItem = NULL;
    if (mHasSearchCache &&
        (cacheItemGuard = mSearchCache->Get((uint64_t)key, options->GetRegionId(), searchCacheCounter)) &&
        (kvCacheItem = cacheItemGuard->GetCacheItem<KVCacheItem>()) &&
        kvCacheItem->IsCacheItemValid(oldestRtSegmentId, options->lastSkipIncTsInSecond)) {
        if (hitBuilding && valueTs >= kvCacheItem->timestamp) {
            FL_CORETURN ret == FOUND_VALID_KEY;
        }
        if (kvCacheItem->nextRtSegmentId >= mBuildingSegmentId) {
            value = autil::MakeCString(kvCacheItem->value, pool);
            valueTs = kvCacheItem->timestamp;
            if (mKVReportMetrics && metricsCollector && kvCacheItem->HasValue()) {
                metricsCollector->IncSearchCacheResultCount(1);
                metricsCollector->IncResultCount();
            }
            FL_CORETURN kvCacheItem->HasValue();
        }
        ret = FL_COAWAIT GetFromRtSegments(options, kvCacheItem->nextRtSegmentId, literalKey, key, value, valueTs, pool,
                                           metricsCollector);
        bool hasValue = false;
        if (ret == NOT_FOUND) {
            value = autil::MakeCString(kvCacheItem->value, pool);
            valueTs = kvCacheItem->timestamp;
            hasValue = kvCacheItem->HasValue();
        } else {
            hasValue = (ret == FOUND_VALID_KEY);
        }
        PutCache(key, options->GetRegionId(), value, mBuildingSegmentId, valueTs, hasValue, options->cachePriority);
        FL_CORETURN hasValue;
    }
    ret = FL_COAWAIT GetFromRtSegments(options, oldestRtSegmentId, literalKey, key, value, valueTs, pool,
                                       metricsCollector);
    if (ret == NOT_FOUND || valueTs < options->incTsInSecond) {
        ret = FL_COAWAIT GetFromOfflineSegments(options, literalKey, key, value, valueTs, pool, metricsCollector);
    }
    bool hasValue = (ret == FOUND_VALID_KEY);
    PutCache(key, options->GetRegionId(), value, mBuildingSegmentId, valueTs, hasValue, options->cachePriority);
    FL_CORETURN hasValue;
}

inline FL_LAZY(KVResultStatus) KVReaderImpl::GetFromSegmentReaders(
    const KVIndexOptions* options, const std::vector<std::vector<OfflineSegmentReader>>& segmentReaders,
    keytype_t literalKey, keytype_t key, autil::StringView& value, uint64_t& valueTs, autil::mem_pool::Pool* pool,
    KVMetricsCollector* metricsCollector) const
{
    if (unlikely(segmentReaders.empty())) {
        FL_CORETURN NOT_FOUND;
    }
    size_t onlineColumnId = util::ColumnUtil::GetColumnId(literalKey, segmentReaders.size());
    for (auto& segmentReader : segmentReaders[onlineColumnId]) {
        bool isDeleted = false;
        if (FL_COAWAIT segmentReader.Get(options, key, value, valueTs, isDeleted, pool, metricsCollector)) {
            if (mKVReportMetrics && metricsCollector) {
                metricsCollector->IncResultCount();
            }
            FL_CORETURN isDeleted ? FOUND_INVALID_KEY : FOUND_VALID_KEY;
        }
    }
    FL_CORETURN NOT_FOUND;
}

inline FL_LAZY(KVResultStatus) KVReaderImpl::GetFromOfflineSegments(const KVIndexOptions* options, keytype_t literalKey,
                                                                    keytype_t key, autil::StringView& value,
                                                                    uint64_t& valueTs, autil::mem_pool::Pool* pool,
                                                                    KVMetricsCollector* metricsCollector) const
{
    FL_CORETURN FL_COAWAIT GetFromSegmentReaders(options, mOfflineSegmentReaders, literalKey, key, value, valueTs, pool,
                                                 metricsCollector);
}

inline FL_LAZY(KVResultStatus) KVReaderImpl::GetFromOnlineSegments(const KVIndexOptions* options, keytype_t literalKey,
                                                                   keytype_t key, autil::StringView& value,
                                                                   uint64_t& valueTs, autil::mem_pool::Pool* pool,
                                                                   KVMetricsCollector* metricsCollector) const
{
    if (!mHasOnlineSegment) {
        FL_CORETURN NOT_FOUND;
    }

    if (mKVReportMetrics && !mHasBuildingSegment && metricsCollector) {
        metricsCollector->BeginSSTableQuery();
    }
    bool isBuildingSegment = mHasBuildingSegment;
    for (auto& segmentReader : mOnlineSegmentReaders) {
        bool isDeleted = false;
        if (FL_COAWAIT segmentReader.Get(options, key, value, valueTs, isDeleted, pool)) {
            if (mKVReportMetrics && metricsCollector) {
                metricsCollector->IncResultCount();
            }
            FL_CORETURN isDeleted ? FOUND_INVALID_KEY : FOUND_VALID_KEY;
        }
        if (mKVReportMetrics && isBuildingSegment && metricsCollector) {
            isBuildingSegment = false;
            metricsCollector->BeginSSTableQuery();
        }
    }
    FL_CORETURN NOT_FOUND;
}

inline FL_LAZY(KVResultStatus) KVReaderImpl::GetFromBuildingSegment(const KVIndexOptions* options, keytype_t literalKey,
                                                                    keytype_t key, autil::StringView& value,
                                                                    uint64_t& valueTs, autil::mem_pool::Pool* pool,
                                                                    KVMetricsCollector* metricsCollector) const
{
    if (mHasBuildingSegment) {
        auto& buildingReader = mOnlineSegmentReaders[0];
        bool isDeleted = false;
        if (FL_COAWAIT buildingReader.Get(options, key, value, valueTs, isDeleted, pool)) {
            if (mKVReportMetrics && metricsCollector) {
                metricsCollector->IncResultCount();
            }
            FL_CORETURN isDeleted ? FOUND_INVALID_KEY : FOUND_VALID_KEY;
        }
    }

    FL_CORETURN NOT_FOUND;
}

inline FL_LAZY(KVResultStatus) KVReaderImpl::GetFromRtSegments(const KVIndexOptions* options,
                                                               segmentid_t nextRtSegmentId, keytype_t literalKey,
                                                               keytype_t key, autil::StringView& value,
                                                               uint64_t& valueTs, autil::mem_pool::Pool* pool,
                                                               KVMetricsCollector* metricsCollector) const
{
    if (!mHasOnlineSegment) {
        FL_CORETURN NOT_FOUND;
    }
    for (size_t i = mHasBuildingSegment ? 1 : 0; i < mOnlineSegmentReaders.size(); ++i) {
        auto& segmentReader = mOnlineSegmentReaders[i];
        if (segmentReader.GetSegmentId() < nextRtSegmentId) {
            break;
        }
        bool isDeleted = false;
        if (FL_COAWAIT segmentReader.Get(options, key, value, valueTs, isDeleted, pool)) {
            if (mKVReportMetrics && metricsCollector) {
                metricsCollector->IncResultCount();
            }
            FL_CORETURN isDeleted ? FOUND_INVALID_KEY : FOUND_VALID_KEY;
        }
    }

    FL_CORETURN NOT_FOUND;
}

inline void KVReaderImpl::PutCache(keytype_t key, regionid_t regionId, const autil::StringView& value,
                                   segmentid_t nextRtSegmentId, uint32_t timestamp, bool hasValue,
                                   autil::CacheBase::Priority priority) const
{
    if (mHasSearchCache) {
        KVCacheItem* cacheItem = KVCacheItem::Create(nextRtSegmentId, timestamp, value);
        cacheItem->SetHasValue(hasValue);
        mSearchCache->Put(key, regionId, cacheItem, priority);
    }
}

inline KVReaderPtr KVReaderImpl::CreateCodegenKVReader()
{
    KVReaderPtr codegenReader;
    CodegenObject* codegenObject = nullptr;
    using createFunc = CodegenObject*();
    CREATE_CODEGEN_OBJECT(codegenObject, createFunc, KVReader);
    if (codegenObject) {
        codegenReader.reset((KVReader*)codegenObject);
    }
    return codegenReader;
}

inline KVReader* KVReaderImpl::Clone() { return new KVReaderImpl(*this); }
}} // namespace indexlib::index

#endif //__INDEXLIB_KV_READER_IMPL_H
