#include <malloc.h>
#include <time.h>
#include <autil/StringUtil.h>
#include "indexlib/partition/memory_stat_reporter.h"
#include "indexlib/partition/memory_stat_collector.h"
#include "indexlib/util/task_item.h"
#include "indexlib/util/task_scheduler.h"
#include "indexlib/util/cache/search_cache_partition_wrapper.h"
#include "indexlib/file_system/file_block_cache.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, MemoryStatReporter);

namespace
{
class MemoryStatReporterTaskItem : public util::TaskItem
{
public:
    MemoryStatReporterTaskItem(
            const MemoryStatCollectorPtr& memStatCollector, uint32_t interval)
        : mMemStatCollector(memStatCollector)
        , mPrintInterval(interval)
        , mPrintTsInSeconds(0)
    {
        mPrintTsInSeconds = TimeUtility::currentTimeInSeconds();
    }

public:
    void Run() override
    {
        mMemStatCollector->ReportMetrics();
        int64_t currentTsInSeconds = TimeUtility::currentTimeInSeconds();
        if (currentTsInSeconds - mPrintTsInSeconds > mPrintInterval)
        {
            char buf[30];
            time_t clock;
            time(&clock);

            struct tm *tm = localtime(&clock);
            strftime(buf, sizeof(buf), " %x %X %Y ", tm);

            cout << "########" << buf << "########" << endl;
            cerr << "########" << buf << "########" << endl;
            malloc_stats();
            mMemStatCollector->PrintMetrics();
            mPrintTsInSeconds = currentTsInSeconds;
        }
    }

private:
    MemoryStatCollectorPtr mMemStatCollector;
    int64_t mPrintInterval;
    int64_t mPrintTsInSeconds;
};


};

MemoryStatReporter::MemoryStatReporter()
    : mPrintMetricsTaskId(TaskScheduler::INVALID_TASK_ID)
{
}

MemoryStatReporter::~MemoryStatReporter() 
{
    if (mTaskScheduler)
    {
        mTaskScheduler->DeleteTask(mPrintMetricsTaskId);
    }
}

bool MemoryStatReporter::Init(const string& param,
                              const SearchCachePartitionWrapperPtr& searchCache,
                              const FileBlockCachePtr& blockCache,
                              const TaskSchedulerPtr& taskScheduler,
                              misc::MetricProviderPtr metricProvider)
{
    mTaskScheduler = taskScheduler;
    int64_t printInterval = 1200;  // 20 min
    if (!param.empty() && !StringUtil::fromString(param, printInterval))
    {
        IE_LOG(INFO, "invalid param [%s] to init MemoryStatReporter", param.c_str());
    }
    mMemStatCollector.reset(new MemoryStatCollector(searchCache, blockCache, metricProvider));
    
    int32_t sleepTime = INDEXLIB_REPORT_METRICS_INTERVAL;
    TaskItemPtr taskItem(new MemoryStatReporterTaskItem(mMemStatCollector, printInterval));
    if (!mTaskScheduler->DeclareTaskGroup("report_metrics", sleepTime))
    {
        IE_LOG(ERROR, "declare report metrics task failed!");
        return false;
    }
    
    mPrintMetricsTaskId = mTaskScheduler->AddTask("report_metrics", taskItem);
    if (mPrintMetricsTaskId == TaskScheduler::INVALID_TASK_ID)
    {
        IE_LOG(ERROR, "add report metrics task failed!");
        return false;
    }
    return true;
}

IE_NAMESPACE_END(partition);

