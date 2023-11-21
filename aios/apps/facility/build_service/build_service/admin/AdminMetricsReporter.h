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

#include "build_service/admin/GenerationMetricsReporter.h"
#include "build_service/common_define.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/util/Log.h"
#include "kmonitor_adapter/Metric.h"
#include "kmonitor_adapter/Monitor.h"

namespace build_service { namespace admin {

class AdminMetricsReporter
{
public:
    class GlobalMetricsRecord
    {
    public:
        GlobalMetricsRecord() { reset(); }

        void reset()
        {
            allGenerationCount = 0;
            activeGenerationCount = 0;
            activeNodeCount = 0;
            assignSlotCount = 0;
            waitSlotNodeCount = 0;
            slowSlotCount = 0;
            failSlotCount = 0;
            keepServiceLoopInterval = 0;
            prohibitedIpCount = 0;
            recoverFailedGenerationCount = 0;
            startingGenerationCount = 0;
            stoppingGenerationCount = 0;
            fullBuildingGenerationCount = 0;
            incBuildingGenerationCount = 0;
        }

    public:
        uint32_t allGenerationCount;
        uint32_t activeGenerationCount;
        uint32_t activeNodeCount;
        uint32_t assignSlotCount;
        uint32_t waitSlotNodeCount;
        uint32_t slowSlotCount;
        uint32_t failSlotCount;
        uint32_t keepServiceLoopInterval;
        uint32_t prohibitedIpCount;
        uint32_t recoverFailedGenerationCount;
        uint32_t startingGenerationCount;
        uint32_t stoppingGenerationCount;
        uint32_t fullBuildingGenerationCount;
        uint32_t incBuildingGenerationCount;
    };

public:
    AdminMetricsReporter();
    ~AdminMetricsReporter();

private:
    AdminMetricsReporter(const AdminMetricsReporter&);
    AdminMetricsReporter& operator=(const AdminMetricsReporter&);

public:
    void init(kmonitor_adapter::Monitor* globalMonitor);

    void reportMetrics(const GlobalMetricsRecord& record);

    GenerationMetricsReporterPtr createGenerationMetrics(const proto::BuildId& buildId);

private:
    bool _enableReportGenerationMetrics;
    kmonitor_adapter::Monitor* _monitor;
    kmonitor_adapter::MetricPtr _allGenerationCountMetric;
    kmonitor_adapter::MetricPtr _activeGenerationCountMetric;
    kmonitor_adapter::MetricPtr _activeNodeCountMetric;
    kmonitor_adapter::MetricPtr _assignSlotCountMetric;
    kmonitor_adapter::MetricPtr _waitSlotNodeCountMetric;
    kmonitor_adapter::MetricPtr _slowSlotCountMetric;
    kmonitor_adapter::MetricPtr _failSlotCountMetric;
    kmonitor_adapter::MetricPtr _keepServiceLoopIntervalMetric;
    kmonitor_adapter::MetricPtr _prohibitedIpCountMetric;
    kmonitor_adapter::MetricPtr _recoverFailedGenerationCountMetric;

    kmonitor_adapter::MetricPtr _startingGenerationCountMetric;
    kmonitor_adapter::MetricPtr _stoppingGenerationCountMetric;
    kmonitor_adapter::MetricPtr _fullBuildingGenerationCountMetric;
    kmonitor_adapter::MetricPtr _incBuildingGenerationCountMetric;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(AdminMetricsReporter);

}} // namespace build_service::admin
