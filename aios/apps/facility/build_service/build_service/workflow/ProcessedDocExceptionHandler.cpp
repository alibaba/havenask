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
#include "build_service/workflow/ProcessedDocExceptionHandler.h"

#include "autil/EnvUtil.h"

using namespace std;

namespace build_service { namespace workflow {
BS_LOG_SETUP(workflow, ProcessedDocExceptionHandler);

ProcessedDocExceptionHandler::ProcessedDocExceptionHandler() {}

ProcessedDocExceptionHandler::~ProcessedDocExceptionHandler() {}

void ProcessedDocExceptionHandler::init(const indexlib::util::MetricProviderPtr& metricProvider)
{
    _processedDocExceptionMetric = DECLARE_METRIC(metricProvider, "basic/parseFailedCount", kmonitor::GAUGE, "count");
    REPORT_METRIC(_processedDocExceptionMetric, _errorCount);
    _treatFailureAsFatal = autil::EnvUtil::getEnv("BS_TREAT_FAILURE_AS_FATAL", false);
    BS_LOG(INFO, "[%s] treat parse failed to fatal error", _treatFailureAsFatal ? "will" : "will not");
}
FlowError ProcessedDocExceptionHandler::transferProcessResult(bool parseSuccess)
{
    if (parseSuccess) {
        return FE_OK;
    }
    _errorCount++;
    REPORT_METRIC(_processedDocExceptionMetric, _errorCount);
    if (_treatFailureAsFatal) {
        BS_LOG(ERROR, "parse failed, will return FE_FATAL, to skip, set env BS_TREAT_FAILURE_AS_FATAL false");
        return FE_FATAL;
    }
    BS_LOG(WARN, "parse failed, will return FE_SKIP, will drop, to make FATAIL, unenv BS_TREAT_FAILURE_AS_FATAL ");
    return FE_SKIP;
}

}} // namespace build_service::workflow
