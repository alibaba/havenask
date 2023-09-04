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

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/framework/IMetrics.h"
#include "indexlib/framework/MetricsWrapper.h"

namespace indexlibv2::framework {

class IndexOperationMetrics : public IMetrics
{
public:
    IndexOperationMetrics(const std::shared_ptr<kmonitor::MetricsReporter>& metricsReporter,
                          const kmonitor::MetricsTags& tags);
    ~IndexOperationMetrics();

public:
    void ReportMetrics() override;
    void RegisterMetrics() override;

    int64_t GetTimerDoneUs();

private:
    INDEXLIB_FM_DECLARE_METRIC(operationExecuteTime);
    std::shared_ptr<kmonitor::MetricsReporter> _metricsReporter;
    autil::ScopedTime2 _timer;
    const kmonitor::MetricsTags _tags;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::framework
