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
#include "build_service/admin/AdminMetricsReporter.h"

#include "autil/EnvUtil.h"
#include "build_service/util/Monitor.h"

using namespace std;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, AdminMetricsReporter);

AdminMetricsReporter::AdminMetricsReporter() : _enableReportGenerationMetrics(false), _monitor(NULL) {}

AdminMetricsReporter::~AdminMetricsReporter() {}

void AdminMetricsReporter::init(kmonitor_adapter::Monitor* monitor)
{
    if (!monitor) {
        return;
    }
    _monitor = monitor;

    _allGenerationCountMetric = monitor->registerGaugeMetric("statistics.allGenerationCount", kmonitor::FATAL);
    _activeGenerationCountMetric = monitor->registerGaugeMetric("statistics.activeGenerationCount", kmonitor::FATAL);
    _activeNodeCountMetric = monitor->registerGaugeMetric("statistics.activeNodeCount", kmonitor::FATAL);
    _assignSlotCountMetric = monitor->registerGaugeMetric("statistics.assignSlotCount", kmonitor::FATAL);
    _waitSlotNodeCountMetric = monitor->registerGaugeMetric("statistics.waitSlotNodeCount", kmonitor::FATAL);
    _slowSlotCountMetric = monitor->registerGaugeMetric("statistics.slowSlotCount", kmonitor::FATAL);
    _failSlotCountMetric = monitor->registerGaugeMetric("statistics.failSlotCount", kmonitor::FATAL);
    _keepServiceLoopIntervalMetric =
        monitor->registerGaugePercentileMetric("statistics.serviceKeeperLoopInterval", kmonitor::FATAL);
    _prohibitedIpCountMetric = monitor->registerGaugeMetric("statistics.prohibitedIpCount", kmonitor::FATAL);
    _recoverFailedGenerationCountMetric =
        monitor->registerGaugeMetric("statistics.recoverFailedGenerationCount", kmonitor::FATAL);

    std::string param = autil::EnvUtil::getEnv(BS_ENV_ENABLE_GENERATION_METRICS.c_str());
    if (param == "true") {
        _enableReportGenerationMetrics = true;
    }

    _startingGenerationCountMetric =
        monitor->registerGaugeMetric("statistics.startingGenerationCount", kmonitor::FATAL);
    _stoppingGenerationCountMetric =
        monitor->registerGaugeMetric("statistics.stoppingGenerationCount", kmonitor::FATAL);
    _fullBuildingGenerationCountMetric =
        monitor->registerGaugeMetric("statistics.fullBuildingGenerationCount", kmonitor::FATAL);
    _incBuildingGenerationCountMetric =
        monitor->registerGaugeMetric("statistics.incBuildingGenerationCount", kmonitor::FATAL);
}

void AdminMetricsReporter::reportMetrics(const GlobalMetricsRecord& record)
{
    REPORT_KMONITOR_METRIC(_allGenerationCountMetric, record.allGenerationCount);
    REPORT_KMONITOR_METRIC(_activeGenerationCountMetric, record.activeGenerationCount);
    REPORT_KMONITOR_METRIC(_activeNodeCountMetric, record.activeNodeCount);
    REPORT_KMONITOR_METRIC(_assignSlotCountMetric, record.assignSlotCount);
    REPORT_KMONITOR_METRIC(_waitSlotNodeCountMetric, record.waitSlotNodeCount);
    REPORT_KMONITOR_METRIC(_slowSlotCountMetric, record.slowSlotCount);
    REPORT_KMONITOR_METRIC(_failSlotCountMetric, record.failSlotCount);
    REPORT_KMONITOR_METRIC(_keepServiceLoopIntervalMetric, record.keepServiceLoopInterval);
    REPORT_KMONITOR_METRIC(_prohibitedIpCountMetric, record.prohibitedIpCount);
    REPORT_KMONITOR_METRIC(_recoverFailedGenerationCountMetric, record.recoverFailedGenerationCount);
    REPORT_KMONITOR_METRIC(_startingGenerationCountMetric, record.startingGenerationCount);
    REPORT_KMONITOR_METRIC(_stoppingGenerationCountMetric, record.stoppingGenerationCount);
    REPORT_KMONITOR_METRIC(_fullBuildingGenerationCountMetric, record.fullBuildingGenerationCount);
    REPORT_KMONITOR_METRIC(_incBuildingGenerationCountMetric, record.incBuildingGenerationCount);
}

GenerationMetricsReporterPtr AdminMetricsReporter::createGenerationMetrics(const proto::BuildId& buildId)
{
    GenerationMetricsReporterPtr generationReportor(new GenerationMetricsReporter(_enableReportGenerationMetrics));
    generationReportor->init(buildId, _monitor);
    return generationReportor;
}

}} // namespace build_service::admin
