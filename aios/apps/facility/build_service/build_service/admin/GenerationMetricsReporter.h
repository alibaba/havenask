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

class GenerationMetricsReporter
{
public:
    class MetricsRecord
    {
    public:
        MetricsRecord() { reset(); }

        void reset()
        {
            activeNodeCount = 0;
            assignSlotCount = 0;
            waitSlotNodeCount = 0;
            totalReleaseSlowSlotCount = 0;
            finishNodeCount = 0;
            totalCpuAmount = 0;
            totalMemAmount = 0;
        }

    public:
        uint32_t activeNodeCount;
        uint32_t assignSlotCount;
        uint32_t waitSlotNodeCount;
        uint32_t totalReleaseSlowSlotCount;
        uint32_t finishNodeCount;
        int64_t totalCpuAmount;
        int64_t totalMemAmount;
    };

public:
    GenerationMetricsReporter(bool enableReportGenerationMetric);
    ~GenerationMetricsReporter();

private:
    GenerationMetricsReporter(const GenerationMetricsReporter&);
    GenerationMetricsReporter& operator=(const GenerationMetricsReporter&);

public:
    void init(const proto::BuildId& buildId, kmonitor_adapter::Monitor* globalMonitor);

    void reportMetrics();

    MetricsRecord& GetMetricsRecord() { return mRecord; }

    void updateMetricsRecord(const MetricsRecord& inRecord) { mRecord = inRecord; }

    void reportPrepareGenerationLatency(uint32_t latency);
    void reportStopGenerationLatency(uint32_t latency);
    void reportFullBuildTime(uint32_t timeInSecond);

private:
    MetricsRecord mRecord;
    bool _enableReportGenerationMetrics;
    kmonitor_adapter::Monitor* _monitor;
    kmonitor_adapter::MetricPtr _totalReleaseSlowSlotCountMetric;
    kmonitor_adapter::MetricPtr _prepareGenerationLatencyMetric;
    kmonitor_adapter::MetricPtr _stopGenerationLatencyMetric;
    kmonitor_adapter::MetricPtr _fullBuildTimeInSecondMetric;
    kmonitor::MetricsTags _tags;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(GenerationMetricsReporter);

}} // namespace build_service::admin
