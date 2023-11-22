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
#include "build_service/admin/GenerationMetricsReporter.h"

#include <cstddef>
#include <string>

#include "autil/Span.h"
#include "autil/StringUtil.h"
#include "build_service/util/Monitor.h"
#include "kmonitor/client/MetricLevel.h"

using namespace std;
using namespace autil;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, GenerationMetricsReporter);

#define METRIC_TAG_GENERATION "generation"
#define METRIC_TAG_APPNAME "appName"
#define METRIC_TAG_DATATABLE "dataTable"

GenerationMetricsReporter::GenerationMetricsReporter(bool enableReportGenerationMetric)
    : _enableReportGenerationMetrics(enableReportGenerationMetric)
    , _monitor(NULL)
{
}

GenerationMetricsReporter::~GenerationMetricsReporter() {}

void GenerationMetricsReporter::init(const proto::BuildId& buildId, kmonitor_adapter::Monitor* globalMonitor)
{
    string generationId = StringUtil::toString(buildId.generationid());
    string appName = buildId.appname();
    string dataTable = buildId.datatable();

    _tags.AddTag(METRIC_TAG_GENERATION, generationId);
    _tags.AddTag(METRIC_TAG_APPNAME, appName);
    _tags.AddTag(METRIC_TAG_DATATABLE, dataTable);

    _monitor = globalMonitor;
    if (_enableReportGenerationMetrics && globalMonitor) {
        _totalReleaseSlowSlotCountMetric =
            _monitor->registerGaugeMetric("statistics.totalReleaseSlowSlotCnt", kmonitor::FATAL);
        _prepareGenerationLatencyMetric =
            _monitor->registerGaugePercentileMetric("statistics.prepareGenerationLatency", kmonitor::FATAL);
        _stopGenerationLatencyMetric =
            _monitor->registerGaugePercentileMetric("statistics.stopGenerationLatency", kmonitor::FATAL);
        _fullBuildTimeInSecondMetric =
            _monitor->registerGaugePercentileMetric("statistics.fullBuildTimeInSecond", kmonitor::FATAL);
    }
}

void GenerationMetricsReporter::reportPrepareGenerationLatency(uint32_t latency)
{
    REPORT_KMONITOR_METRIC2(_prepareGenerationLatencyMetric, _tags, latency);
}

void GenerationMetricsReporter::reportStopGenerationLatency(uint32_t latency)
{
    REPORT_KMONITOR_METRIC2(_stopGenerationLatencyMetric, _tags, latency);
}

void GenerationMetricsReporter::reportFullBuildTime(uint32_t timeInSecond)
{
    REPORT_KMONITOR_METRIC2(_fullBuildTimeInSecondMetric, _tags, timeInSecond);
}

void GenerationMetricsReporter::reportMetrics()
{
    if (_enableReportGenerationMetrics) {
        REPORT_KMONITOR_METRIC2(_totalReleaseSlowSlotCountMetric, _tags, mRecord.totalReleaseSlowSlotCount);
    }
}

}} // namespace build_service::admin
