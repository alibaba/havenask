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
#ifndef __INDEXLIB_INDEX_PARTITION_MERGER_METRICS_H
#define __INDEXLIB_INDEX_PARTITION_MERGER_METRICS_H

#include <memory>

#include "autil/Lock.h"
#include "autil/Thread.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/metrics/MetricProvider.h"
#include "indexlib/util/metrics/Monitor.h"
#include "indexlib/util/metrics/ProgressMetrics.h"
#include "kmonitor/client/core/MetricsTags.h"

DECLARE_REFERENCE_CLASS(util, CounterMap);
DECLARE_REFERENCE_CLASS(util, StateCounter);

namespace indexlib { namespace merger {

class IndexPartitionMergerMetrics
{
public:
    IndexPartitionMergerMetrics(util::MetricProviderPtr metricProvider = util::MetricProviderPtr());
    ~IndexPartitionMergerMetrics();

public:
    util::ProgressMetricsPtr RegisterMergeItem(double total,
                                               const kmonitor::MetricsTags& _tags = kmonitor::MetricsTags())
    {
        util::ProgressMetricsPtr metrics(new util::ProgressMetrics(total));
        autil::ScopedLock lock(mLock);
        mMergeItemMetrics.push_back(metrics);
        kmonitor::MetricsTagsPtr tags(new kmonitor::MetricsTags());
        _tags.MergeTags((kmonitor::MetricsTags*)tags.get());
        mMergeItemTags.push_back(tags);
        return metrics;
    }

    void StartReport(const util::CounterMapPtr& counterMap = util::CounterMapPtr());
    void StopReport();

    void BeginRecaculatorRunningTime();
    void EndRecaculatorRunningTime();

    void SetMergeVersionInfo(const std::string& schemaName, int32_t fromVersion, int32_t targetVersion,
                             const std::vector<int32_t>& mergeFrom, const std::vector<int32_t>& mergeTo);

private:
    void ReportMetrics();
    void ReportLoop();

private:
    IE_DECLARE_METRIC(progress);
    IE_DECLARE_METRIC(leftItemCount);
    IE_DECLARE_METRIC(mergeItemRunningTime);
    IE_DECLARE_METRIC(recalculateLatency);
    IE_DECLARE_METRIC(fromVersionId);
    IE_DECLARE_METRIC(targetVersionId);

private:
    util::MetricProviderPtr mMetricProvider;
    autil::RecursiveThreadMutex mLock;
    util::StateCounterPtr mProgressCounter;
    util::StateCounterPtr mLeftItemCounter;
    std::vector<util::ProgressMetricsPtr> mMergeItemMetrics;
    std::vector<kmonitor::MetricsTagsPtr> mMergeItemTags;
    autil::ThreadPtr mReportThread;
    volatile bool mRunning;
    volatile int64_t mBeginTs;
    volatile int64_t mRecalculateBeginTs;

    int32_t mMergeFromVersion;
    int32_t mMergeTargetVersion;
    std::vector<kmonitor::MetricsTagsPtr> mMergeFromVersionTags;
    std::vector<kmonitor::MetricsTagsPtr> mMergeTargetVersionTags;

private:
    friend class IndexPartitionMergerMetricsTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexPartitionMergerMetrics);
}} // namespace indexlib::merger

#endif //__INDEXLIB_INDEX_PARTITION_MERGER_METRICS_H
