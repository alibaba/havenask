#ifndef __INDEXLIB_BLOCK_CACHE_H
#define __INDEXLIB_BLOCK_CACHE_H

#include <tr1/memory>
#include "indexlib/misc/metric_provider.h"
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/cache/cache.h"
#include "indexlib/util/cache/block.h"
#include "indexlib/misc/monitor.h"
#include "indexlib/util/timer.h"
#include "indexlib/util/latency_stat.h"
#include "indexlib/util/metric_reporter.h"
#include "indexlib/file_system/read_option.h"

DECLARE_REFERENCE_CLASS(util, BlockAllocator);

IE_NAMESPACE_BEGIN(util);

class BlockCache
{
public:
    BlockCache();
    ~BlockCache();

public:
    bool Init(size_t cacheSize, uint32_t blockSize, int32_t shardBitsNum = 6,
              bool useClockCache = false, bool useprefetch = false);
    bool Put(Block* block, Cache::Handle** handle);
    Block* Get(const blockid_t& blockId, Cache::Handle** handle);
    void ReleaseHandle(Cache::Handle* handle);

    const BlockAllocatorPtr& GetBlockAllocator() const { return mBlockAllocator; }    
    size_t GetCacheSize() const { return mCacheSize; }
    uint32_t GetBlockSize() const { return mBlockSize; }
    uint32_t GetBlockCount() const { return mCache->GetUsage() / mBlockSize; }
    uint32_t GetMaxBlockCount() const { return mCache->GetCapacity() / mBlockSize; }
    uint64_t GetMemoryUse() const { return mCache ? mCache->GetUsage() : 0; }

    int64_t GetTotalHitCount() { return mBlockCacheHitReporter.GetTotalCount(); }
    int64_t GetTotalMissCount() { return mBlockCacheMissReporter.GetTotalCount(); }
    bool IsUsePrefetcher() { return mUsePrefetcher; }
public:
    // just global cache support now
    void RegisterMetrics(misc::MetricProviderPtr metricProvider);
    void ReportHit(file_system::BlockAccessCounter* counter = nullptr)
    {
        mHitRatioReporter.Record(100);
        mBlockCacheHitReporter.IncreaseQps(1);
        // IE_REPORT_METRIC(BlockCacheHitRatio, 100);
        if (counter)
        {
            counter->blockCacheHitCount++;
        }
    }
    void ReportMiss(file_system::BlockAccessCounter* counter = nullptr)
    {
        mHitRatioReporter.Record(0);
        mBlockCacheMissReporter.IncreaseQps(1);
        // IE_REPORT_METRIC(BlockCacheHitRatio, 0);
        if (counter)
        {
            counter->blockCacheMissCount++;
        }        
    }
    void ReportReadLatency(uint64_t readLatency, file_system::BlockAccessCounter* counter = nullptr)
    {
        mReadLatencyReporter.Record(static_cast<double>(readLatency));
        if (counter)
        {
            counter->blockCacheReadLatency += readLatency;
            counter->blockCacheIOCount++;
        }
    }

    void ReportReadBlockCount(uint64_t count)
    {
        /* assert(count); */
        /* IE_REPORT_METRIC(BlockCacheReadCount, count); */
        /* if (count == 1) */
        /* { */
        /*     IE_INCREASE_QPS(BlockCacheReadOneBlockQps); */
        /* } */
        /* else */
        /* { */
        /*     IE_INCREASE_QPS(BlockCacheReadMultiBlockQps); */
        /* } */
    }
    void ReportPercentile()
    {
        // // duration: 10^-6 sec
        // int64_t duration = mTimer.Stop();
        // // last calc 10 seconds ago
        // if (duration >= 10000000){
        //     mTimer.RestartTimer();
        //     mLatencyStat.GetPercentile(mPerList, mAnsList);
        // }
        // IE_REPORT_METRIC(LatencyPercentile90, mAnsList[0]);
        // IE_REPORT_METRIC(LatencyPercentile95, mAnsList[1]);
        // IE_REPORT_METRIC(LatencyPercentile99, mAnsList[2]);
        // IE_REPORT_METRIC(LatencyPercentile995, mAnsList[3]);
        // IE_REPORT_METRIC(LatencyPercentile999, mAnsList[4]);
    }

    void ReportReadSize(int64_t size, file_system::BlockAccessCounter* counter = nullptr)
    {
        if (counter)
        {
            counter->blockCacheIODataSize += size;
        }
    }
    void ReportPrefetchCount(uint64_t count)
    {
        mBlockCachePrefetchReporter.IncreaseQps(count);
    }
    
    void ReportMetrics()
    {
        mBlockCacheHitReporter.Report();
        mBlockCacheMissReporter.Report();
        mHitRatioReporter.Report();
        mReadLatencyReporter.Report();
        mBlockCachePrefetchReporter.Report();
    }

public:
    uint32_t TEST_GetRefCount(Cache::Handle* handle);
    const char* TEST_GetCacheName() const { return mCache->Name(); }

private:
    BlockAllocatorPtr mBlockAllocator;
    std::shared_ptr<Cache> mCache;
    size_t mCacheSize;
    uint32_t mBlockSize;
    bool mUsePrefetcher;
    //three percentiles
    const std::vector<float> mPerList = {90, 95, 99, 99.5, 99.9};
    std::vector<uint32_t> mAnsList = {0, 0, 0, 0, 0};

private:
    IE_DECLARE_METRIC(BlockCacheHitRatio);
    IE_DECLARE_METRIC(BlockCacheHitQps);
    IE_DECLARE_METRIC(BlockCacheMissQps);
    IE_DECLARE_METRIC(BlockCachePrefetchQps);
    IE_DECLARE_METRIC(BlockCacheReadLatency);
    IE_DECLARE_METRIC(BlockCacheReadCount);
    IE_DECLARE_METRIC(LatencyPercentile90);
    IE_DECLARE_METRIC(LatencyPercentile95);
    IE_DECLARE_METRIC(LatencyPercentile99);
    IE_DECLARE_METRIC(LatencyPercentile995);
    IE_DECLARE_METRIC(LatencyPercentile999);
    IE_DECLARE_METRIC(BlockCacheReadOneBlockQps);
    IE_DECLARE_METRIC(BlockCacheReadMultiBlockQps);

    InputMetricReporter mHitRatioReporter;
    InputMetricReporter mReadLatencyReporter;
    
    QpsMetricReporter mBlockCacheHitReporter;
    QpsMetricReporter mBlockCacheMissReporter;
    QpsMetricReporter mBlockCachePrefetchReporter;

    Timer mTimer;
    LatencyStat mLatencyStat;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BlockCache);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_BLOCK_CACHE_H
