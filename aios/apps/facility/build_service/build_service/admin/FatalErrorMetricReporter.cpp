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
#include "build_service/admin/FatalErrorMetricReporter.h"

#include <cstddef>
#include <string>

#include "alog/Logger.h"
#include "autil/Span.h"
#include "autil/StringUtil.h"
#include "build_service/util/Monitor.h"
#include "kmonitor/client/MetricLevel.h"

using namespace std;
using namespace autil;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, FatalErrorMetricReporter);

#define METRIC_TAG_GENERATION "generation"
#define METRIC_TAG_APPNAME "appName"
#define METRIC_TAG_DATATABLE "dataTable"

FatalErrorMetricReporter::FatalErrorMetricReporter() {}

FatalErrorMetricReporter::~FatalErrorMetricReporter() {}

void FatalErrorMetricReporter::init(const proto::BuildId& buildId, kmonitor_adapter::Monitor* monitor)
{
    if (monitor == NULL) {
        BS_LOG(INFO, "init FatalErrorMetricReporter failed");
        return;
    }

    BS_LOG(INFO, "init FatalErrorMetricReporter");
    string generationId = StringUtil::toString(buildId.generationid());
    string appName = buildId.appname();
    string dataTable = buildId.datatable();

    _tags.AddTag(METRIC_TAG_GENERATION, generationId);
    _tags.AddTag(METRIC_TAG_APPNAME, appName);
    _tags.AddTag(METRIC_TAG_DATATABLE, dataTable);

    _fatalErrorDurationMetric = monitor->registerGaugeMetric("fatal.fatalErrorDuration", kmonitor::FATAL);
}

void FatalErrorMetricReporter::reportFatalError(int64_t duration, const kmonitor::MetricsTags& tags)
{
    kmonitor::MetricsTags newTags(_tags.GetTagsMap());
    tags.MergeTags(&newTags);
    REPORT_KMONITOR_METRIC2(_fatalErrorDurationMetric, newTags, duration);
}

}} // namespace build_service::admin
