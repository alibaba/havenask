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
#include "indexlib/index/util/build_profiling_metrics.h"

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/raw_document/default_raw_document.h"

using namespace std;
using namespace indexlib::util;
using namespace indexlib::config;
using namespace indexlib::document;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, BuildProfilingMetrics);

BuildProfilingMetrics::BuildProfilingMetrics()
    : mAddIndexTime(0)
    , mAddAttributeTime(0)
    , mAddSummaryTime(0)
    , mBuildDocCount(0)
    , mReportBatchSize(DEFAULT_REPORT_BATCH_SIZE)
{
}

BuildProfilingMetrics::~BuildProfilingMetrics() {}

bool BuildProfilingMetrics::Init(const IndexPartitionSchemaPtr& schema, const MetricProviderPtr& metricProvider)
{
    if (!!autil::EnvUtil::getEnv("indexlib_enable_build_profiling", false)) {
        return false;
    }

    mReportBatchSize = autil::EnvUtil::getEnv("indexlib_build_profiling_step", mReportBatchSize);
    IE_LOG(INFO, "enable build profiling with profiling step [%ld].", mReportBatchSize);

    mSchema = schema;
    return RegisterMetrics(schema, metricProvider);
}

bool BuildProfilingMetrics::RegisterMetrics(const IndexPartitionSchemaPtr& schema,
                                            const MetricProviderPtr& metricProvider)
{
    if (!schema || !metricProvider) {
        return false;
    }

    if (mSchema->GetSubIndexPartitionSchema()) {
        IE_LOG(WARN, "build profiling not support sub schema.");
        return false;
    }

    mSchema = schema;
    mMetricProvider = metricProvider;
    IE_INIT_METRIC_GROUP(mMetricProvider, buildDocumentLatency, "profiling/buildDocumentLatency", kmonitor::GAUGE,
                         "us");

    if (mSchema->GetTableType() == tt_index) {
        IE_INIT_METRIC_GROUP(mMetricProvider, addIndexLatency, "profiling/addIndexLatency", kmonitor::GAUGE, "us");
        IE_INIT_METRIC_GROUP(mMetricProvider, addAttributeLatency, "profiling/addAttributeLatency", kmonitor::GAUGE,
                             "us");
        IE_INIT_METRIC_GROUP(mMetricProvider, addSummaryLatency, "profiling/addSummaryLatency", kmonitor::GAUGE, "us");
        IE_INIT_METRIC_GROUP(mMetricProvider, addSingleIndexLatency, "profiling/addSingleIndexLatency", kmonitor::GAUGE,
                             "us");
        IE_INIT_METRIC_GROUP(mMetricProvider, addSingleAttributeLatency, "profiling/addSingleAttributeLatency",
                             kmonitor::GAUGE, "us");
    }
    return true;
}

void BuildProfilingMetrics::Reset()
{
    mBuildDocCount = 0;
    mAddIndexTime = 0;
    mAddAttributeTime = 0;
    mAddSummaryTime = 0;

    mBuildDocTime.clear();
    mAddSingleIndexTimes.clear();
    mAddSingleAttributeTimes.clear();
}

void BuildProfilingMetrics::Report()
{
    if (mBuildDocCount <= 0) {
        return;
    }

    for (size_t i = 0; i < mBuildDocTime.size(); i++) {
        int64_t count = mBuildDocTime[i].first;
        int64_t time = mBuildDocTime[i].second;
        if (count <= 0) {
            continue;
        }

        DocOperateType type = (DocOperateType)i;
        kmonitor::MetricsTags tag("schema_name", mSchema->GetSchemaName());
        tag.AddTag("operate_type", DefaultRawDocument::getCmdString(type));
        IE_REPORT_METRIC_WITH_TAGS(buildDocumentLatency, &tag, time / count);
        if (type == ADD_DOC) {
            ReportAddDocMetrics(count);
        }
    }
}

void BuildProfilingMetrics::ReportAddDocMetrics(int64_t count)
{
    kmonitor::MetricsTags tag("schema_name", mSchema->GetSchemaName());
    if (mAddIndexTime > 0) {
        IE_REPORT_METRIC_WITH_TAGS(addIndexLatency, &tag, mAddIndexTime / count);
    }
    if (mAddAttributeTime > 0) {
        IE_REPORT_METRIC_WITH_TAGS(addAttributeLatency, &tag, mAddAttributeTime / count);
    }
    if (mAddSummaryTime > 0) {
        IE_REPORT_METRIC_WITH_TAGS(addSummaryLatency, &tag, mAddSummaryTime / count);
    }

    IndexSchemaPtr indexSchema = mSchema->GetIndexSchema();
    if (indexSchema) {
        for (indexid_t id = 0; id < mAddSingleIndexTimes.size(); id++) {
            IndexConfigPtr indexConf = indexSchema->GetIndexConfig(id);
            if (!indexConf) {
                continue;
            }

            if (mAddSingleIndexTimes[id] > 0) {
                kmonitor::MetricsTags indexTag("schema_name", mSchema->GetSchemaName());
                indexTag.AddTag("index", indexConf->GetIndexName());
                IE_REPORT_METRIC_WITH_TAGS(addSingleIndexLatency, &indexTag, mAddSingleIndexTimes[id] / count);
            }
        }
    }

    AttributeSchemaPtr attrSchema = mSchema->GetAttributeSchema();
    if (attrSchema) {
        for (attrid_t id = 0; id < mAddSingleAttributeTimes.size(); id++) {
            auto attrConf = attrSchema->GetAttributeConfig(id);
            if (!attrConf) {
                continue;
            }

            if (mAddSingleAttributeTimes[id] > 0) {
                kmonitor::MetricsTags attrTag("schema_name", mSchema->GetSchemaName());
                attrTag.AddTag("attribute", attrConf->GetAttrName());
                IE_REPORT_METRIC_WITH_TAGS(addSingleAttributeLatency, &attrTag, mAddSingleAttributeTimes[id] / count);
            }
        }
    }
}

void BuildProfilingMetrics::ReportSingleDocBuildTime(const DocumentPtr& doc, int64_t time)
{
    if (!doc) {
        return;
    }

    size_t typeIdx = (size_t)doc->GetDocOperateType();
    if (typeIdx >= mBuildDocTime.size()) {
        mBuildDocTime.resize(typeIdx + 1);
        mBuildDocTime[typeIdx].first = 0;
        mBuildDocTime[typeIdx].second = 0;
    }

    mBuildDocTime[typeIdx].first++;
    mBuildDocTime[typeIdx].second += time;
    ++mBuildDocCount;

    if (mBuildDocCount < mReportBatchSize) {
        return;
    }

    Report();
    Reset();
}

void BuildProfilingMetrics::CollectAddIndexTime(int64_t time) { mAddIndexTime += time; }

void BuildProfilingMetrics::CollectAddAttributeTime(int64_t time) { mAddAttributeTime += time; }

void BuildProfilingMetrics::CollectAddSummaryTime(int64_t time) { mAddSummaryTime += time; }

void BuildProfilingMetrics::CollectAddSingleIndexTime(indexid_t indexId, int64_t time)
{
    if (indexId == INVALID_INDEXID) {
        return;
    }

    if ((size_t)indexId >= mAddSingleIndexTimes.size()) {
        mAddSingleIndexTimes.resize(indexId + 1);
        mAddSingleIndexTimes[indexId] = 0;
    }
    mAddSingleIndexTimes[indexId] += time;
}

void BuildProfilingMetrics::CollectAddSingleAttributeTime(attrid_t attrId, int64_t time)
{
    if (attrId == INVALID_ATTRID) {
        return;
    }

    if ((size_t)attrId >= mAddSingleAttributeTimes.size()) {
        mAddSingleAttributeTimes.resize(attrId + 1);
        mAddSingleAttributeTimes[attrId] = 0;
    }
    mAddSingleAttributeTimes[attrId] += time;
}
}} // namespace indexlib::index
