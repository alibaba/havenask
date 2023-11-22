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
#include "build_service/workflow/ReportFreshnessMetricTaskItem.h"

#include <assert.h>
#include <string>

#include "build_service/workflow/SwiftProcessedDocProducer.h"
#include "indexlib/base/Constant.h"
#include "indexlib/indexlib.h"

namespace build_service { namespace workflow {
BS_LOG_SETUP(workflow, ReportFreshnessMetricTaskItem);

ReportFreshnessMetricTaskItem::ReportFreshnessMetricTaskItem(SwiftProcessedDocProducer* producer) : _producer(producer)
{
}

void ReportFreshnessMetricTaskItem::Run()
{
    assert(_producer);
    static const std::string emptyDocSource("");
    _producer->reportFreshnessMetrics(INVALID_TIMESTAMP, /* no more msg */ false, emptyDocSource,
                                      /* report fast queue delay */ true);
}

}} // namespace build_service::workflow
