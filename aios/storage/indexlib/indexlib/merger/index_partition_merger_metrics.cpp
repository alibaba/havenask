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
#include "indexlib/merger/index_partition_merger_metrics.h"

#include <ext/alloc_traits.h>
#include <functional>
#include <iosfwd>
#include <map>
#include <memory>
#include <unistd.h>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "beeper/beeper.h"
#include "beeper/common/common_type.h"
#include "indexlib/util/ErrorLogCollector.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/counter/StateCounter.h"
#include "indexlib/util/metrics/Metric.h"
#include "kmonitor/client/MetricType.h"

using namespace std;
using namespace autil;
using namespace indexlib::util;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, IndexPartitionMergerMetrics);

IndexPartitionMergerMetrics::IndexPartitionMergerMetrics(MetricProviderPtr metricProvider)
    : mMetricProvider(metricProvider)
    , mRunning(false)
    , mBeginTs(0)
    , mRecalculateBeginTs(-1)
    , mMergeFromVersion(-1)
    , mMergeTargetVersion(-1)
{
    IE_INIT_METRIC_GROUP(mMetricProvider, progress, "merge/progress", kmonitor::STATUS, "%");
    IE_INIT_METRIC_GROUP(mMetricProvider, leftItemCount, "merge/leftItemCount", kmonitor::STATUS, "count");
    IE_INIT_METRIC_GROUP(mMetricProvider, mergeItemRunningTime, "merge/doMergeItemRunningTime", kmonitor::GAUGE, "us");
    IE_INIT_METRIC_GROUP(mMetricProvider, recalculateLatency, "merge/recalculateLatency", kmonitor::GAUGE, "us");
    IE_INIT_METRIC_GROUP(mMetricProvider, fromVersionId, "merge/fromVersionId", kmonitor::GAUGE, "count");
    IE_INIT_METRIC_GROUP(mMetricProvider, targetVersionId, "merge/targetVersionId", kmonitor::GAUGE, "count");
}

IndexPartitionMergerMetrics::~IndexPartitionMergerMetrics() { StopReport(); }

void IndexPartitionMergerMetrics::BeginRecaculatorRunningTime()
{
    mRecalculateBeginTs = autil::TimeUtility::currentTime();
}

void IndexPartitionMergerMetrics::EndRecaculatorRunningTime()
{
    int64_t recalculateBeginTs = mRecalculateBeginTs;
    if (recalculateBeginTs > 0) {
        IE_REPORT_METRIC(recalculateLatency, autil::TimeUtility::currentTime() - recalculateBeginTs);
    }
    mRecalculateBeginTs = -1;
}

void IndexPartitionMergerMetrics::SetMergeVersionInfo(const string& schemaName, int32_t fromVersion,
                                                      int32_t targetVersion, const vector<int32_t>& mergeFrom,
                                                      const vector<int32_t>& mergeTo)
{
    autil::ScopedLock lock(mLock);
    mMergeFromVersion = fromVersion;
    mMergeTargetVersion = targetVersion;

    for (auto id : mergeFrom) {
        map<string, string> kvMap = {{"schema_name", schemaName}, {"from_segment", autil::StringUtil::toString(id)}};
        kmonitor::MetricsTagsPtr tags(new kmonitor::MetricsTags(kvMap));
        mMergeFromVersionTags.push_back(tags);
    }

    for (auto id : mergeTo) {
        map<string, string> kvMap = {{"schema_name", schemaName}, {"target_segment", autil::StringUtil::toString(id)}};
        kmonitor::MetricsTagsPtr tags(new kmonitor::MetricsTags(kvMap));
        mMergeTargetVersionTags.push_back(tags);
    }
}

void IndexPartitionMergerMetrics::ReportMetrics()
{
    double total = 0;
    double current = 0;
    size_t leftItemCount = 0;

    autil::ScopedLock lock(mLock);
    for (size_t i = 0; i < mMergeItemMetrics.size(); i++) {
        total += mMergeItemMetrics[i]->GetTotal();
        current += mMergeItemMetrics[i]->GetCurrent();
        if (!mMergeItemMetrics[i]->IsFinished()) {
            ++leftItemCount;
            int64_t startTime = mMergeItemMetrics[i]->GetStartTimestamp();
            if (startTime > 0) {
                int64_t runningTime = autil::TimeUtility::currentTime() - startTime;
                IE_REPORT_METRIC_WITH_TAGS(mergeItemRunningTime, mMergeItemTags[i], runningTime);
            }
        }
    }

    double progress = 100.0;
    if (likely(total > 0.0f && current < total)) {
        progress = current * 100.0 / total;
    }

    IE_REPORT_METRIC(progress, progress);
    IE_REPORT_METRIC(leftItemCount, leftItemCount);
    if (mProgressCounter) {
        mProgressCounter->Set(int64_t(progress));
    }
    if (mLeftItemCounter) {
        mLeftItemCounter->Set(int64_t(leftItemCount));
    }

    if (mMergeFromVersion > 0) {
        for (size_t i = 0; i < mMergeFromVersionTags.size(); i++) {
            IE_REPORT_METRIC_WITH_TAGS(fromVersionId, mMergeFromVersionTags[i], mMergeFromVersion);
        }
    }
    if (mMergeTargetVersion > 0) {
        for (size_t i = 0; i < mMergeTargetVersionTags.size(); i++) {
            IE_REPORT_METRIC_WITH_TAGS(targetVersionId, mMergeTargetVersionTags[i], mMergeTargetVersion);
        }
    }

    beeper::EventTags tags;
    BEEPER_FORMAT_INTERVAL_REPORT(300, "bs_worker_status", tags,
                                  "mergeItem total [%lu], left [%lu], progress [%ld/100]", mMergeItemMetrics.size(),
                                  leftItemCount, (int64_t)progress);
    int64_t now = TimeUtility::currentTimeInSeconds();
    if (mRunning && now - mBeginTs > 3600) // over 1 hour
    {
        BEEPER_FORMAT_INTERVAL_REPORT(600, "bs_worker_error", tags,
                                      "do merger not finish after running over 1 hour, cost [%ld] seconds."
                                      "mergeItem total [%lu], left [%lu], progress [%ld/100]",
                                      now - mBeginTs, mMergeItemMetrics.size(), leftItemCount, (int64_t)progress);
    }
}

void IndexPartitionMergerMetrics::StartReport(const CounterMapPtr& counterMap)
{
    if (mRunning) {
        IE_LOG(ERROR, "Already StartReport!");
        return;
    }

    ScopedLock lock(mLock);
    if (counterMap) {
        mProgressCounter = counterMap->GetStateCounter("offline.mergeProgress");
        if (!mProgressCounter) {
            IE_LOG(ERROR, "init counter[offline.mergeProgress] failed");
        }
        mLeftItemCounter = counterMap->GetStateCounter("offline.leftItemCount");
        if (!mLeftItemCounter) {
            IE_LOG(ERROR, "init counter[offline.leftItemCount] failed");
        }
    }

    ReportMetrics();
    mRunning = true;
    mBeginTs = TimeUtility::currentTimeInSeconds();
    mReportThread =
        autil::Thread::createThread(std::bind(&IndexPartitionMergerMetrics::ReportLoop, this), "indexReport");
}

void IndexPartitionMergerMetrics::StopReport()
{
    if (!mRunning) {
        return;
    }

    ReportMetrics();
    mRunning = false;
    mReportThread.reset();
}

void IndexPartitionMergerMetrics::ReportLoop()
{
    while (mRunning) {
        ReportMetrics();
        usleep(500);
    }
}
}} // namespace indexlib::merger
