#ifndef __INDEXLIB_SEARCH_CACHE_H
#define __INDEXLIB_SEARCH_CACHE_H

#include <tr1/memory>
#include <autil/ConstString.h>
#include <autil/mem_pool/PoolBase.h>
#include "indexlib/misc/metric_provider.h"
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/cache/lru_cache.h"
#include "indexlib/util/memory_control/memory_quota_controller.h"
#include "indexlib/misc/monitor.h"
#include "indexlib/util/counter/accumulative_counter.h"
#include "indexlib/util/metric_reporter.h"

DECLARE_REFERENCE_CLASS(util, TaskScheduler);

IE_NAMESPACE_BEGIN(util);

class CacheItemGuard
{
public:
    CacheItemGuard(Cache::Handle* handle, LRUCache& cache)
        : mHandle(handle)
        , mCache(cache)
    {}
    ~CacheItemGuard()
    {
        if (mHandle)
        {
            mCache.Release(mHandle);
            mHandle = NULL;
        }
    }
private:
    CacheItemGuard(const CacheItemGuard& other);
    CacheItemGuard& operator = (const CacheItemGuard& other);
public:
    template<typename CacheItem>
    CacheItem* GetCacheItem() const
    { return mHandle ? (CacheItem*)(mCache.Value(mHandle)) : NULL; }
private:
    Cache::Handle* mHandle;
    LRUCache& mCache;
};

class SearchCache
{
public:
    SearchCache(size_t cacheSize,
                const MemoryQuotaControllerPtr& memoryQuotaController,
                const TaskSchedulerPtr& taskScheduler,
                misc::MetricProviderPtr metricProvider,
                int numShardBits);
    ~SearchCache();
public:
    template<typename CacheItem>
    bool Put(const autil::ConstString& key, CacheItem* cacheItem);

    std::unique_ptr<CacheItemGuard> Get(const autil::ConstString& key);

    size_t GetCacheItemCount() const { return 0; }
    size_t GetUsage() const { return mLRUCache.GetUsage(); }
    size_t GetCacheSize() const { return mCacheSize; }

    void ResetCounter();
    int64_t GetHitCount() const { return mHitCounter.GetLocal(); }
    int64_t GetMissCount() const { return mMissCounter.GetLocal(); }
    void ReportMetrics()
    {
        mHitRatioReporter.Report();
        mHitReporter.Report();
        mMissReporter.Report();
    }

private:
    void Init(const MemoryQuotaControllerPtr& memoryQuotaController,
              const TaskSchedulerPtr& taskScheduler = TaskSchedulerPtr(),
              misc::MetricProviderPtr metricProvider = misc::MetricProviderPtr());
    bool RegisterMetricsReporter(misc::MetricProviderPtr metricProvider);
private:
    LRUCache mLRUCache;
    size_t mCacheSize;

    MemoryQuotaControllerPtr mMemoryQuotaController;
    TaskSchedulerPtr mTaskScheduler;
    int32_t mReportMetricsTaskId;

    AccumulativeCounter mHitCounter;
    AccumulativeCounter mMissCounter;

private:
    IE_DECLARE_METRIC(SearchCacheHitRatio);
    IE_DECLARE_METRIC(SearchCacheHitQps);
    IE_DECLARE_METRIC(SearchCacheMissQps);

    InputMetricReporter mHitRatioReporter;
    QpsMetricReporter mHitReporter;
    QpsMetricReporter mMissReporter;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SearchCache);

//////////////////////////////////////////////////////////////////////

template<typename CacheItem>
inline bool SearchCache::Put(const autil::ConstString& key, CacheItem* cacheItem)
{
    Status status = mLRUCache.Insert(key, cacheItem,
            cacheItem->Size(), CacheItem::Deleter, NULL,
            Cache::Priority::LOW);
    return status.ok();
}

inline std::unique_ptr<CacheItemGuard> SearchCache::Get(
        const autil::ConstString& key)
{
    Cache::Handle* handle = mLRUCache.Lookup(key);
    if (handle)
    {
        mHitReporter.IncreaseQps(1);
        mHitRatioReporter.Record(100);
        mHitCounter.Increase(1);
        return std::unique_ptr<CacheItemGuard>(new CacheItemGuard(handle, mLRUCache));
    }
    mMissReporter.IncreaseQps(1);
    mHitRatioReporter.Record(0);
    mMissCounter.Increase(1);
    return std::unique_ptr<CacheItemGuard>();
}

inline void SearchCache::ResetCounter()
{
    mHitCounter.Reset();
    mMissCounter.Reset();
}

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_SEARCH_CACHE_H
