#include <unistd.h>
#include <beeper/beeper.h>
#include "indexlib/merger/index_partition_merger_metrics.h"
#include "indexlib/util/counter/counter_map.h"
#include "indexlib/util/counter/state_counter.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, IndexPartitionMergerMetrics);

IndexPartitionMergerMetrics::IndexPartitionMergerMetrics(
        MetricProviderPtr metricProvider)
    : mMetricProvider(metricProvider)
    , mRunning(false)
{
    IE_INIT_METRIC_GROUP(mMetricProvider, progress,
                         "merge/progress", kmonitor::STATUS, "%");
    IE_INIT_METRIC_GROUP(mMetricProvider, leftItemCount,
                         "merge/leftItemCount", kmonitor::STATUS, "count");
}

IndexPartitionMergerMetrics::~IndexPartitionMergerMetrics() 
{
    StopReport();
}

void IndexPartitionMergerMetrics::ReportMetrics()
{
    double total = 0;
    double current = 0;
    size_t leftItemCount = 0;

    autil::ScopedLock lock(mLock);
    for (size_t i = 0; i < mMergeItemMetrics.size(); i++)
    {
        total += mMergeItemMetrics[i]->GetTotal();
        current += mMergeItemMetrics[i]->GetCurrent();
        if (!mMergeItemMetrics[i]->IsFinished())
        {
            ++leftItemCount;
        }
    }
        
    double progress = 100.0;
    if (likely(total > 0.0f && current < total))
    {
        progress = current * 100.0 / total;
    }
        
    IE_REPORT_METRIC(progress, progress);
    IE_REPORT_METRIC(leftItemCount, leftItemCount);
    if (mProgressCounter)
    {
        mProgressCounter->Set(int64_t(progress));
    }
    if (mLeftItemCounter)
    {
        mLeftItemCounter->Set(int64_t(leftItemCount));
    }

    beeper::EventTags tags;
    BEEPER_FORMAT_INTERVAL_REPORT(300, "bs_worker_status", tags,
                                  "mergeItem total [%lu], left [%lu], progress [%ld/100]",
                                  mMergeItemMetrics.size(), leftItemCount, (int64_t)progress);
    int64_t now = TimeUtility::currentTimeInSeconds();
    if (now - mBeginTs > 3600) // over 1 hour
    {
        BEEPER_FORMAT_INTERVAL_REPORT(600, "bs_worker_error", tags,
                "do merger not finish after running over 1 hour, cost [%ld] seconds."
                "mergeItem total [%lu], left [%lu], progress [%ld/100]",
                now - mBeginTs, mMergeItemMetrics.size(), leftItemCount, (int64_t)progress);
    }
}

void IndexPartitionMergerMetrics::StartReport(const CounterMapPtr& counterMap)
{
    if (mRunning)
    {
        IE_LOG(ERROR, "Already StartReport!");
        return;
    }

    ScopedLock lock(mLock);
    if (counterMap)
    {
        mProgressCounter = counterMap->GetStateCounter("offline.mergeProgress");
        if (!mProgressCounter)
        {
            IE_LOG(ERROR, "init counter[offline.mergeProgress] failed");
        }
        mLeftItemCounter = counterMap->GetStateCounter("offline.leftItemCount");
        if (!mLeftItemCounter)
        {
            IE_LOG(ERROR, "init counter[offline.leftItemCount] failed");
        }
    }
    
    ReportMetrics();
    mRunning = true;
    mBeginTs = TimeUtility::currentTimeInSeconds();
    mReportThread = Thread::createThread(
            std::tr1::bind(&IndexPartitionMergerMetrics::ReportLoop, this), "indexReport");
}

void IndexPartitionMergerMetrics::StopReport()
{
    if (!mRunning)
    {
        return;
    }

    ReportMetrics();
    mRunning = false;
    mReportThread.reset();
}

void IndexPartitionMergerMetrics::ReportLoop()
{
    while (mRunning)
    {
        ReportMetrics();
        usleep(500);
    }
}

IE_NAMESPACE_END(merger);

