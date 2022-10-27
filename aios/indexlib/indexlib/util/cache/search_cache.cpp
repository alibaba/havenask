#include "indexlib/util/cache/search_cache.h"
#include "indexlib/util/cache/search_cache_task_item.h"
#include "indexlib/util/task_scheduler.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, SearchCache);

SearchCache::SearchCache(size_t cacheSize,
                         const MemoryQuotaControllerPtr& memoryQuotaController,
                         const TaskSchedulerPtr& taskScheduler,
                         MetricProviderPtr metricProvider,
                         int numShardBits)
    : mLRUCache(cacheSize, numShardBits, true, 0.0, CacheAllocatorPtr())
    , mCacheSize(cacheSize)
    , mReportMetricsTaskId(TaskScheduler::INVALID_TASK_ID)
{
    Init(memoryQuotaController, taskScheduler, metricProvider);
}

SearchCache::~SearchCache() 
{
    if (mMemoryQuotaController)
    {
        mMemoryQuotaController->Free(mCacheSize);
    }
    if (mTaskScheduler)
    {
        mTaskScheduler->DeleteTask(mReportMetricsTaskId);
    }
}

void SearchCache::Init(const MemoryQuotaControllerPtr& memoryQuotaController,
                       const TaskSchedulerPtr& taskScheduler,
                       MetricProviderPtr metricProvider)
{
    mMemoryQuotaController = memoryQuotaController;
    if (mMemoryQuotaController)
    {
        mMemoryQuotaController->Allocate(mCacheSize);
    }
    
    mTaskScheduler = taskScheduler;
    RegisterMetricsReporter(metricProvider);
}

bool SearchCache::RegisterMetricsReporter(misc::MetricProviderPtr metricProvider)
{
    IE_INIT_METRIC_GROUP(metricProvider, SearchCacheHitRatio,
                         "global/SearchCacheHitRatio", kmonitor::GAUGE, "%");
    IE_INIT_LOCAL_INPUT_METRIC(mHitRatioReporter, SearchCacheHitRatio);
    IE_INIT_METRIC_GROUP(metricProvider, SearchCacheHitQps,
                         "global/SearchCacheHitQps", kmonitor::QPS, "count");
    IE_INIT_METRIC_GROUP(metricProvider, SearchCacheMissQps,
                         "global/SearchCacheMissQps", kmonitor::QPS, "count");

    mHitReporter.Init(mSearchCacheHitQpsMetric);
    mMissReporter.Init(mSearchCacheMissQpsMetric);

    if (!mTaskScheduler || !metricProvider)
    {
        return true;
    }
    int32_t sleepTime = INDEXLIB_REPORT_METRICS_INTERVAL;
    if (!mTaskScheduler->DeclareTaskGroup("report_metrics", sleepTime))
    {
        IE_LOG(ERROR, "search cache declare report metrics task failed!");
        return false;
    }
    TaskItemPtr reportMetricsTask(new SearchCacheTaskItem(
                    this, metricProvider));
    mReportMetricsTaskId = mTaskScheduler->AddTask("report_metrics", reportMetricsTask);
    if (mReportMetricsTaskId == TaskScheduler::INVALID_TASK_ID)
    {
        IE_LOG(ERROR, "search cache add report metrics task failed!");
        return false;
    }
    return true;
}


IE_NAMESPACE_END(util);

