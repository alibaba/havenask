#ifndef __INDEXLIB_INDEX_PARTITION_MERGER_METRICS_H
#define __INDEXLIB_INDEX_PARTITION_MERGER_METRICS_H

#include <tr1/memory>
#include "indexlib/misc/metric_provider.h"
#include <autil/Thread.h>
#include <autil/Lock.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/misc/monitor.h"
#include "indexlib/util/progress_metrics.h"

DECLARE_REFERENCE_CLASS(util, CounterMap);
DECLARE_REFERENCE_CLASS(util, StateCounter);

IE_NAMESPACE_BEGIN(merger);

class IndexPartitionMergerMetrics
{
public:
    IndexPartitionMergerMetrics(
            misc::MetricProviderPtr metricProvider = misc::MetricProviderPtr());
    ~IndexPartitionMergerMetrics();
    
public:
    util::ProgressMetricsPtr RegisterMergeItem(double total)
    {
        util::ProgressMetricsPtr metrics(new util::ProgressMetrics(total));
        autil::ScopedLock lock(mLock);
        mMergeItemMetrics.push_back(metrics);
        return metrics;
    }

    void StartReport(const util::CounterMapPtr& counterMap = util::CounterMapPtr());
    void StopReport();
    
private:
    void ReportMetrics();
    void ReportLoop();

private:
    IE_DECLARE_METRIC(progress);
    IE_DECLARE_METRIC(leftItemCount);

private:
    misc::MetricProviderPtr mMetricProvider;
    autil::RecursiveThreadMutex mLock; 
    util::StateCounterPtr mProgressCounter;
    util::StateCounterPtr mLeftItemCounter;
    std::vector<util::ProgressMetricsPtr> mMergeItemMetrics;
    autil::ThreadPtr mReportThread;
    volatile bool mRunning;
    volatile int64_t mBeginTs;
    
private:
    friend class IndexPartitionMergerMetricsTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexPartitionMergerMetrics);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_INDEX_PARTITION_MERGER_METRICS_H
