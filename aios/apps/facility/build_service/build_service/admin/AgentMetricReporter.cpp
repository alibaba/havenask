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
#include "build_service/admin/AgentMetricReporter.h"

#include "autil/StringUtil.h"
#include "build_service/util/Monitor.h"

using namespace std;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, AgentMetricReporter);

AgentMetricReporter::AgentMetricReporter(kmonitor_adapter::Monitor* monitor, bool isGlobalAgent)
    : _isGlobalAgent(isGlobalAgent)
{
    if (monitor) {
        string prefix = isGlobalAgent ? "global_agent." : "generation_agent.";
        _candidateRoleCountMetric = monitor->registerGaugeMetric(prefix + "candidateRoleCount", kmonitor::FATAL);
        _assignedRoleCountMetric = monitor->registerGaugeMetric(prefix + "assignedRoleCount", kmonitor::FATAL);
        _waitScheduleRoleCountMetric = monitor->registerGaugeMetric(prefix + "waitScheduleRoleCount", kmonitor::FATAL);

        _totalAgentNodeCountMetric = monitor->registerGaugeMetric(prefix + "totalAgentNodeCount", kmonitor::FATAL);
        _assignAgentNodeCountMetric = monitor->registerGaugeMetric(prefix + "assignAgentNodeCount", kmonitor::FATAL);
        _idleAgentNodeCountMetric = monitor->registerGaugeMetric(prefix + "idleAgentNodeCount", kmonitor::FATAL);
        _busyAgentNodeCountMetric = monitor->registerGaugeMetric(prefix + "busyAgentNodeCount", kmonitor::FATAL);
        _readyAgentNodeCountMetric = monitor->registerGaugeMetric(prefix + "readyAgentNodeCount", kmonitor::FATAL);
    }
}

AgentMetricReporter::~AgentMetricReporter() {}

void AgentMetricReporter::reportMetric(const AgentGroupScheduleInfo& info)
{
    for (auto& [_, data] : info) {
        kmonitor::MetricsTags tags;
        if (_isGlobalAgent) {
            tags.AddTag("global_id", data.buildId.datatable());
            tags.AddTag("group_id", data.groupId);
        } else {
            tags.AddTag("data_table", data.buildId.datatable());
            tags.AddTag("app_name", data.buildId.appname());
            tags.AddTag("generation_id", autil::StringUtil::toString(data.buildId.generationid()));
            tags.AddTag("group_id", data.groupId);
        }
        tags.AddTag("dynamic_mapping", data.isDynamicMapping ? "true" : "false");
        REPORT_KMONITOR_METRIC2(_candidateRoleCountMetric, tags, data.totalRoleCnt);
        REPORT_KMONITOR_METRIC2(_assignedRoleCountMetric, tags, data.assignRoleCnt);
        REPORT_KMONITOR_METRIC2(_waitScheduleRoleCountMetric, tags, data.totalRoleCnt - data.assignRoleCnt);
        REPORT_KMONITOR_METRIC2(_totalAgentNodeCountMetric, tags, data.totalAgentNodeCnt);
        REPORT_KMONITOR_METRIC2(_assignAgentNodeCountMetric, tags, data.assignAgentNodeCnt);
        REPORT_KMONITOR_METRIC2(_idleAgentNodeCountMetric, tags, data.idleAgentNodeCnt);
        REPORT_KMONITOR_METRIC2(_readyAgentNodeCountMetric, tags, data.readyAgentNodeCnt);
        REPORT_KMONITOR_METRIC2(_busyAgentNodeCountMetric, tags, data.totalAgentNodeCnt - data.idleAgentNodeCnt);
    }
}

}} // namespace build_service::admin
