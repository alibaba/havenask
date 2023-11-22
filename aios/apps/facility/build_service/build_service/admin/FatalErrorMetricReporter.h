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
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/util/Log.h"
#include "kmonitor/client/core/MetricsTags.h"
#include "kmonitor_adapter/Metric.h"
#include "kmonitor_adapter/Monitor.h"

namespace build_service { namespace admin {

class FatalErrorMetricReporter
{
public:
    FatalErrorMetricReporter();
    ~FatalErrorMetricReporter();

    FatalErrorMetricReporter(const FatalErrorMetricReporter&) = delete;
    FatalErrorMetricReporter& operator=(const FatalErrorMetricReporter&) = delete;
    FatalErrorMetricReporter(FatalErrorMetricReporter&&) = delete;
    FatalErrorMetricReporter& operator=(FatalErrorMetricReporter&&) = delete;

public:
    void init(const proto::BuildId& buildId, kmonitor_adapter::Monitor* globalMonitor);
    void reportFatalError(int64_t duration, const kmonitor::MetricsTags& tags);

private:
    kmonitor::MetricsTags _tags;
    kmonitor_adapter::MetricPtr _fatalErrorDurationMetric;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(FatalErrorMetricReporter);

}} // namespace build_service::admin
