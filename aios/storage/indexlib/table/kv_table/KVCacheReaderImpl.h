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

#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/table/kv_table/KVCacheItem.h"
#include "indexlib/table/kv_table/KVReaderImpl.h"
#include "indexlib/util/cache/SearchCachePartitionWrapper.h"

namespace indexlibv2::table {

class KVCacheReaderImpl final : public KVReaderImpl
{
public:
    KVCacheReaderImpl(schemaid_t readerSchemaId) : KVReaderImpl(readerSchemaId) {}

    void SetSearchCache(const indexlib::util::SearchCachePartitionWrapperPtr& searchCache);

protected:
    FL_LAZY(index::KVResultStatus)
    DoGet(const index::KVReadOptions* readOptions, index::keytype_t key, autil::StringView& value, uint64_t& valueTs,
          index::KVMetricsCollector* metricsCollector = NULL) const noexcept override;

private:
    index::KVResultStatus GetFromCache(index::keytype_t key, autil::StringView& value, uint64_t& valueTs,
                                       autil::mem_pool::Pool* pool,
                                       index::KVMetricsCollector* metricsCollector) const noexcept;
    void PutCache(index::keytype_t key, const autil::StringView& value,
                  const std::shared_ptr<framework::Locator>& locator, uint32_t timestamp, bool hasValue,
                  autil::CacheBase::Priority priority) const noexcept;
    std::shared_ptr<framework::Locator> GetLastDiskSegmentLocator(size_t shardId) const;

private:
    indexlib::util::SearchCachePartitionWrapperPtr _searchCache;
};

///////////////////////////////////////////////////////////////////////

inline FL_LAZY(index::KVResultStatus)
    KVCacheReaderImpl::DoGet(const index::KVReadOptions* readOptions, index::keytype_t key, autil::StringView& value,
                             uint64_t& valueTs, index::KVMetricsCollector* metricsCollector) const noexcept
{
    if (nullptr == _searchCache || readOptions->searchCacheType == indexlib::tsc_no_cache) {
        FL_CORETURN FL_COAWAIT KVReaderImpl::DoGet(readOptions, key, value, valueTs, metricsCollector);
    }

    ResetCounter(metricsCollector);
    autil::mem_pool::Pool* pool = readOptions->pool;
    if (readOptions->searchCacheType == indexlib::tsc_only_cache) {
        FL_CORETURN GetFromCache(key, value, valueTs, pool, metricsCollector);
    }
    auto timeoutTerminator = readOptions->timeoutTerminator.get();
    auto shardId = GetShardId(key);
    auto status = FL_COAWAIT GetFromMemSegment(key, value, valueTs, shardId, pool, metricsCollector, timeoutTerminator);
    if (status != index::KVResultStatus::NOT_FOUND) {
        FL_CORETURN status;
    }

    auto lastDiskSegmentLocator = GetLastDiskSegmentLocator(shardId);
    indexlib::util::SearchCacheCounter* searchCacheCounter = nullptr;
    if (_kvReportMetrics && metricsCollector) {
        metricsCollector->BeginSSTableQuery();
        searchCacheCounter = metricsCollector->GetSearchCacheCounter();
    }
    std::unique_ptr<indexlib::util::CacheItemGuard> cacheItemGuard;
    KVCacheItem* kvCacheItem = NULL;
    if ((cacheItemGuard = _searchCache->Get((uint64_t)key, 0, searchCacheCounter)) &&
        (kvCacheItem = cacheItemGuard->GetCacheItem<KVCacheItem>())) {
        status = FL_COAWAIT GetFromDiskSegments(kvCacheItem->GetLocator(), key, value, valueTs, shardId, pool,
                                                metricsCollector, timeoutTerminator);
        if (status == index::KVResultStatus::FAIL) {
            FL_CORETURN status;
        }
        bool hasValue = false;
        bool needCache = (nullptr != lastDiskSegmentLocator);
        if (status == index::KVResultStatus::NOT_FOUND) {
            value = autil::MakeCString(kvCacheItem->GetValue(), pool);
            valueTs = kvCacheItem->GetTimestamp();
            hasValue = kvCacheItem->HasValue();
            if (_kvReportMetrics && metricsCollector && kvCacheItem->HasValue()) {
                metricsCollector->IncSearchCacheResultCount(1);
                metricsCollector->IncResultCount();
            }
            bool locatorSame = (*lastDiskSegmentLocator) == (*kvCacheItem->GetLocator());
            if (nullptr != lastDiskSegmentLocator && locatorSame) {
                needCache = false;
            }
        } else {
            hasValue = (status == index::KVResultStatus::FOUND);
        }
        if (needCache) {
            PutCache(key, value, lastDiskSegmentLocator, valueTs, hasValue, autil::CacheBase::Priority::LOW);
        }
        FL_CORETURN hasValue ? index::KVResultStatus::FOUND : index::KVResultStatus::NOT_FOUND;
    }
    status = FL_COAWAIT GetFromDiskSegments(nullptr, key, value, valueTs, shardId, pool, metricsCollector,
                                            timeoutTerminator);
    if (status == index::KVResultStatus::FAIL) {
        FL_CORETURN status;
    }
    bool hasValue = (status == index::KVResultStatus::FOUND);
    if (lastDiskSegmentLocator) {
        PutCache(key, value, lastDiskSegmentLocator, valueTs, hasValue, autil::CacheBase::Priority::LOW);
    }
    FL_CORETURN hasValue ? index::KVResultStatus::FOUND : index::KVResultStatus::NOT_FOUND;
}

inline index::KVResultStatus KVCacheReaderImpl::GetFromCache(index::keytype_t key, autil::StringView& value,
                                                             uint64_t& valueTs, autil::mem_pool::Pool* pool,
                                                             index::KVMetricsCollector* metricsCollector) const noexcept
{
    indexlib::util::SearchCacheCounter* searchCacheCounter = nullptr;
    if (_kvReportMetrics && metricsCollector) {
        metricsCollector->BeginSSTableQuery();
        searchCacheCounter = metricsCollector->GetSearchCacheCounter();
    }
    std::unique_ptr<indexlib::util::CacheItemGuard> cacheItemGuard;
    KVCacheItem* kvCacheItem = NULL;
    if ((cacheItemGuard = _searchCache->Get((uint64_t)key, 0, searchCacheCounter)) &&
        (kvCacheItem = cacheItemGuard->GetCacheItem<KVCacheItem>())) {
        value = autil::MakeCString(kvCacheItem->GetValue(), pool);
        valueTs = kvCacheItem->GetTimestamp();
        if (_kvReportMetrics && metricsCollector && kvCacheItem->HasValue()) {
            metricsCollector->IncSearchCacheResultCount(1);
            metricsCollector->IncResultCount();
        }
        return kvCacheItem->HasValue() ? index::KVResultStatus::FOUND : index::KVResultStatus::NOT_FOUND;
    }
    return index::KVResultStatus::NOT_FOUND;
}

inline void KVCacheReaderImpl::PutCache(index::keytype_t key, const autil::StringView& value,
                                        const std::shared_ptr<framework::Locator>& locator, uint32_t timestamp,
                                        bool hasValue, autil::CacheBase::Priority priority) const noexcept
{
    KVCacheItem* cacheItem = KVCacheItem::Create(locator, timestamp, value);
    cacheItem->SetHasValue(hasValue);
    _searchCache->Put(key, 0, cacheItem, priority);
}

inline std::shared_ptr<framework::Locator> KVCacheReaderImpl::GetLastDiskSegmentLocator(size_t shardId) const
{
    if (!_diskShardReaders.empty() && !_diskShardReaders[shardId].empty()) {
        return _diskShardReaders[shardId][0].second;
    }
    return nullptr;
}

inline void KVCacheReaderImpl::SetSearchCache(const indexlib::util::SearchCachePartitionWrapperPtr& searchCache)
{
    if (!searchCache) {
        return;
    }

    if (!_kvIndexConfig) {
        _searchCache = searchCache;
        return;
    }

    std::string id = autil::StringUtil::toString(_readerSchemaId) + "@";
    id += _kvIndexConfig->GetIndexName() + ":";
    auto valueConfig = _kvIndexConfig->GetValueConfig();
    size_t attrCount = valueConfig->GetAttributeCount();
    for (size_t i = 0; i < attrCount; i++) {
        auto attrConfig = valueConfig->GetAttributeConfig(i);
        id += attrConfig->GetAttrName() + ";";
    }
    _searchCache.reset(searchCache->CreateExclusiveCacheWrapper(id));
}

} // namespace indexlibv2::table
