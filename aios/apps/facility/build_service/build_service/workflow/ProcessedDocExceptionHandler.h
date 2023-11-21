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
#pragma once

#include <stdint.h>

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/workflow/FlowError.h"
#include "indexlib/util/metrics/Metric.h"
#include "indexlib/util/metrics/MetricProvider.h"

namespace build_service { namespace workflow {

class ProcessedDocExceptionHandler
{
public:
    ProcessedDocExceptionHandler();
    ~ProcessedDocExceptionHandler();

public:
    void init(const indexlib::util::MetricProviderPtr& metricProvider);
    FlowError transferProcessResult(bool processSuccess);

private:
    bool _treatFailureAsFatal = true;
    uint32_t _errorCount = 0;
    indexlib::util::MetricPtr _processedDocExceptionMetric;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ProcessedDocExceptionHandler);

}} // namespace build_service::workflow
