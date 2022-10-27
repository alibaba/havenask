#include <autil/HashAlgorithm.h>
#include <autil/StringUtil.h>
#include <autil/MurmurHash.h>
#include "indexlib/file_system/file_block_cache.h"
#include "indexlib/util/cache/block_cache.h"
#include "indexlib/file_system/block_cache_metrics.h"
#include "indexlib/file_system/file_block_cache_task_item.h"
#include "indexlib/util/memory_control/simple_memory_quota_controller.h"
#include "indexlib/util/task_scheduler.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, FileBlockCache);

FileBlockCache::FileBlockCache() 
    : mReportMetricsTaskId(TaskScheduler::INVALID_TASK_ID)
    , TEST_mQuickExit(false)
{
}

FileBlockCache::~FileBlockCache() 
{
    if (mGlobalMemoryQuotaController && mBlockCache)
    {
        mGlobalMemoryQuotaController->Free(GetMemoryUse());
    }
    if (mTaskScheduler)
    {
        mTaskScheduler->DeleteTask(mReportMetricsTaskId);
    }
}

bool FileBlockCache::Init(const std::string& configStr,
                          const util::MemoryQuotaControllerPtr& globalMemoryQuotaController,
                          const util::TaskSchedulerPtr& taskScheduler,
                          misc::MetricProviderPtr metricProvider)
{
    mGlobalMemoryQuotaController = globalMemoryQuotaController;
    mTaskScheduler = taskScheduler;

    vector<vector<string>> paramVec;
    StringUtil::fromString(configStr, paramVec,
                           _CONFIG_KV_SEPERATOR, _CONFIG_SEPERATOR);
    size_t cacheSize = _DEFAULT_CACHE_SIZE;
    uint32_t blockSize = _DEFAULT_BLOCK_SIZE;
    int32_t shardBitsNum = 6;
    constexpr bool useClockCache = false; // not supported in this version
    bool useprefetch = false;
    for (size_t i = 0; i < paramVec.size(); ++i)
    {
        if (paramVec[i].size() != 2)
        {
            IE_LOG(ERROR, "parse block cache param[%s] failed", configStr.c_str());
            return false;
        }
        if (paramVec[i][0] == _CONFIG_CACHE_SIZE_NAME
            && StringUtil::fromString(paramVec[i][1], cacheSize))
        {
            continue;
        }
        else if (paramVec[i][0] == _CONFIG_BLOCK_SIZE_NAME
                 && StringUtil::fromString(paramVec[i][1], blockSize))
        {
            continue;
        }
        else if (paramVec[i][0] == _CONFIG_NUM_SHARD_BITS
                 && StringUtil::fromString(paramVec[i][1], shardBitsNum))
        {
            continue;
        }
        else if (paramVec[i][0] == _CONFIG_USE_PREFETCHER
                 && StringUtil::fromString(paramVec[i][1], useprefetch))
        {
            continue;
        }
        IE_LOG(ERROR, "parse block cache param[%s] failed", configStr.c_str());
        return false;
    }
    if (shardBitsNum < 0 || shardBitsNum > 30)
    {
        IE_LOG(ERROR, "parse block cache param failed, config [%s]num_shard_bits[%d]",
               configStr.c_str(), shardBitsNum);
        return false;
    }
    mBlockCache.reset(new util::BlockCache);
    cacheSize *= (1024 * 1024); // MB to B

    if (!mBlockCache->Init(cacheSize, blockSize, shardBitsNum, useClockCache, useprefetch))
    {
        IE_LOG(ERROR, "init file block cache failed with[%s]: "
               "cacheSize[%lu], blockSize[%u], clockCache[%d]",
               configStr.c_str(), cacheSize, blockSize, useClockCache);
        return false;
    }

    if (mGlobalMemoryQuotaController)
    {
        mGlobalMemoryQuotaController->Allocate(GetMemoryUse());
    }

    IE_LOG(INFO, "init file block cache with[%s]: "
           "cacheSize[%lu], blockSize[%u], clockCache[%d], use block prefetcher [%d]",
           configStr.c_str(), cacheSize, blockSize, useClockCache, useprefetch);

    return RegisterMetricsReporter(metricProvider);
}

bool FileBlockCache::Init(size_t cacheSize, uint32_t blockSize,
                          const util::SimpleMemoryQuotaControllerPtr& quotaController)
{
    if (cacheSize < blockSize)
    {
        IE_LOG(ERROR, "cache size [%lu] lees than block size [%u] ",
               cacheSize, blockSize);
        return false;
    }
    mCacheMemController = quotaController;

    mBlockCache.reset(new util::BlockCache);
    if (!mBlockCache->Init(cacheSize, blockSize))
    {
        IE_LOG(ERROR, "Init cache failed");
        return false;
    }

    if (mCacheMemController)
    {
        mCacheMemController->Allocate(GetMemoryUse());
    }
    return true;
}

bool FileBlockCache::RegisterMetricsReporter(misc::MetricProviderPtr metricProvider)
{
    if (!mTaskScheduler || !metricProvider)
    {
        return true;
    }
    
    int32_t sleepTime = INDEXLIB_REPORT_METRICS_INTERVAL;
    if (unlikely(TEST_mQuickExit))
    {
        sleepTime /= 1000;
    }
    if (!mTaskScheduler->DeclareTaskGroup("report_metrics", sleepTime))
    {
        IE_LOG(ERROR, "global file block cache declare report metrics task failed!");
        return false;
    }
    TaskItemPtr reportMetricsTask(new FileBlockCacheTaskItem(
                    mBlockCache.get(), metricProvider));
    mReportMetricsTaskId = mTaskScheduler->AddTask("report_metrics", reportMetricsTask);
    if (mReportMetricsTaskId == TaskScheduler::INVALID_TASK_ID)
    {
        IE_LOG(ERROR, "global file block cache add report metrics task failed!");
        return false;
    }
    mBlockCache->RegisterMetrics(metricProvider);
    return true;
}

size_t FileBlockCache::GetMemoryUse() const
{
    return mBlockCache->GetCacheSize();
}

uint64_t FileBlockCache::GetFileId(const string& fileName) 
{
    return MurmurHash::MurmurHash64A(
            fileName.c_str(), fileName.length(), 545609244UL);
}

size_t FileBlockCache::GetCacheSize() const
{
    return mBlockCache->GetCacheSize();
}

size_t FileBlockCache::GetUsage() const
{
    return mBlockCache->GetMemoryUse();
}

void FileBlockCache::ReportMetrics() { return mBlockCache->ReportMetrics(); }

IE_NAMESPACE_END(file_system);

