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

#include "autil/ConstString.h"
#include "autil/Log.h"
#include "autil/cache/cache.h"
#include "autil/mem_pool/PoolBase.h"
#include "indexlib/util/cache/SearchCacheCounter.h"
#include "indexlib/util/counter/AccumulativeCounter.h"
#include "indexlib/util/memory_control/MemoryQuotaController.h"
#include "indexlib/util/metrics/MetricProvider.h"
#include "indexlib/util/metrics/MetricReporter.h"
#include "indexlib/util/metrics/Monitor.h"

namespace indexlib { namespace util {

class TaskScheduler;

class CacheItemGuard
{
public:
    CacheItemGuard(autil::CacheBase::Handle* handle, autil::CacheBase& cache) : _handle(handle), _cache(cache) {}
    ~CacheItemGuard()
    {
        if (_handle) {
            _cache.Release(_handle);
            _handle = NULL;
        }
    }

private:
    CacheItemGuard(const CacheItemGuard& other);
    CacheItemGuard& operator=(const CacheItemGuard& other);

public:
    template <typename CacheItem>
    CacheItem* GetCacheItem() const
    {
        return _handle ? (CacheItem*)(_cache.Value(_handle)) : NULL;
    }

private:
    autil::CacheBase::Handle* _handle;
    autil::CacheBase& _cache;
};

class SearchCache
{
public:
    SearchCache(size_t cacheSize, const MemoryQuotaControllerPtr& memoryQuotaController,
                const std::shared_ptr<TaskScheduler>& taskScheduler, util::MetricProviderPtr metricProvider,
                int numShardBits, float highPriorityRatio = 0.0f);
    SearchCache(size_t cacheSize, const MemoryQuotaControllerPtr& memoryQuotaController,
                const std::shared_ptr<TaskScheduler>& taskScheduler, util::MetricProviderPtr metricProvider,
                int numShardBits, float highPriorityRatio, float lowPriorityRatio);
    ~SearchCache();

public:
    template <typename CacheItem>
    bool Put(const autil::StringView& key, CacheItem* cacheItem, autil::CacheBase::Priority priority);

    std::unique_ptr<CacheItemGuard> Get(const autil::StringView& key, SearchCacheCounter* searchCacheCounter = nullptr);

    size_t GetCacheItemCount() const { return 0; }
    size_t GetUsage() const { return _cache->GetUsage(); }
    size_t GetCacheSize() const { return _cacheSize; }

    void ReportMetrics()
    {
        _hitRatioReporter.Report();
        _hitReporter.Report();
        _missReporter.Report();
    }

private:
    void Init(const MemoryQuotaControllerPtr& memoryQuotaController,
              const std::shared_ptr<TaskScheduler>& taskScheduler = std::shared_ptr<TaskScheduler>(),
              util::MetricProviderPtr metricProvider = util::MetricProviderPtr());
    bool RegisterMetricsReporter(util::MetricProviderPtr metricProvider);

private:
    std::shared_ptr<autil::CacheBase> _cache;
    size_t _cacheSize;

    MemoryQuotaControllerPtr _memoryQuotaController;
    std::shared_ptr<TaskScheduler> _taskScheduler;
    int32_t _reportMetricsTaskId;

private:
    IE_DECLARE_METRIC(SearchCacheHitRatio);
    IE_DECLARE_METRIC(SearchCacheHitQps);
    IE_DECLARE_METRIC(SearchCacheMissQps);

    InputMetricReporter _hitRatioReporter;
    QpsMetricReporter _hitReporter;
    QpsMetricReporter _missReporter;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SearchCache> SearchCachePtr;

//////////////////////////////////////////////////////////////////////

template <typename CacheItem>
inline bool SearchCache::Put(const autil::StringView& key, CacheItem* cacheItem, autil::CacheBase::Priority priority)
{
    return _cache->Insert(key, cacheItem, cacheItem->Size(), CacheItem::Deleter, NULL, priority);
}

inline std::unique_ptr<CacheItemGuard> SearchCache::Get(const autil::StringView& key,
                                                        SearchCacheCounter* searchCacheCounter)
{
    autil::CacheBase::Handle* handle = _cache->Lookup(key);
    if (handle) {
        _hitReporter.IncreaseQps(1);
        _hitRatioReporter.Record(100);

        if (searchCacheCounter) {
            searchCacheCounter->hitCount++;
        }
        return std::unique_ptr<CacheItemGuard>(new CacheItemGuard(handle, *_cache));
    }
    _missReporter.IncreaseQps(1);
    _hitRatioReporter.Record(0);

    if (searchCacheCounter) {
        searchCacheCounter->missCount++;
    }
    return std::unique_ptr<CacheItemGuard>();
}
}} // namespace indexlib::util
