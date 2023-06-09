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
#ifndef __INDEXLIB_BUILD_PROFILING_METRICS_H
#define __INDEXLIB_BUILD_PROFILING_METRICS_H

#include <memory>

#include "autil/EnvUtil.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/metrics/Monitor.h"

DECLARE_REFERENCE_CLASS(document, Document);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);

namespace indexlib { namespace index {

///////////////////////////////////////////////////////////////////////
class BuildProfilingMetrics
{
public:
    BuildProfilingMetrics();
    ~BuildProfilingMetrics();

public:
    // TODO: support sub schema
    bool Init(const config::IndexPartitionSchemaPtr& schema, const util::MetricProviderPtr& metricProvider);

    void CollectAddIndexTime(int64_t time);
    void CollectAddAttributeTime(int64_t time);
    void CollectAddSummaryTime(int64_t time);

    void CollectAddSingleIndexTime(indexid_t indexId, int64_t time);
    void CollectAddSingleAttributeTime(attrid_t attrId, int64_t time);

    void ReportSingleDocBuildTime(const document::DocumentPtr& doc, int64_t time);

private:
    bool RegisterMetrics(const config::IndexPartitionSchemaPtr& schema, const util::MetricProviderPtr& metricProvider);
    void Report();
    void ReportAddDocMetrics(int64_t docCount);
    void Reset();

private:
    int64_t mAddIndexTime;
    int64_t mAddAttributeTime;
    int64_t mAddSummaryTime;

    // first: count, second: time
    typedef std::pair<int64_t, int64_t> SingleDocInfo;
    std::vector<SingleDocInfo> mBuildDocTime;

    std::vector<int64_t> mAddSingleIndexTimes;
    std::vector<int64_t> mAddSingleAttributeTimes;

    int64_t mBuildDocCount;
    int64_t mReportBatchSize;

    util::MetricProviderPtr mMetricProvider;
    config::IndexPartitionSchemaPtr mSchema;

    IE_DECLARE_METRIC(buildDocumentLatency);
    IE_DECLARE_METRIC(addIndexLatency);
    IE_DECLARE_METRIC(addAttributeLatency);
    IE_DECLARE_METRIC(addSummaryLatency);
    IE_DECLARE_METRIC(addSingleIndexLatency);
    IE_DECLARE_METRIC(addSingleAttributeLatency);

    const static int64_t DEFAULT_REPORT_BATCH_SIZE = 100;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BuildProfilingMetrics);
////////////////////////////////////////////////////////////////////////////
class ScopeProfilingDocumentReporter
{
public:
    ScopeProfilingDocumentReporter(const document::DocumentPtr& doc, BuildProfilingMetricsPtr& metric)
        : _beginTime(0)
        , _doc(doc)
        , _metric(metric)
    {
        if (_metric) {
            _beginTime = autil::TimeUtility::currentTime();
        }
    }

    ~ScopeProfilingDocumentReporter()
    {
        if (_metric) {
            int64_t endTime = autil::TimeUtility::currentTime();
            _metric->ReportSingleDocBuildTime(_doc, endTime - _beginTime);
        }
    }

private:
    int64_t _beginTime;
    const document::DocumentPtr& _doc;
    BuildProfilingMetricsPtr& _metric;
};
}} // namespace indexlib::index

#endif //__INDEXLIB_BUILD_PROFILING_METRICS_H
