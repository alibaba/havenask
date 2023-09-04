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

#include "build_service/admin/AgentRolePlanMaker.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "kmonitor_adapter/Monitor.h"

namespace build_service { namespace admin {

class AgentMetricReporter
{
public:
    using AgentGroupScheduleInfo = AgentRolePlanMaker::AgentGroupScheduleInfo;

public:
    AgentMetricReporter(kmonitor_adapter::Monitor* monitor, bool isGlobalAgent);
    ~AgentMetricReporter();

public:
    void reportMetric(const AgentGroupScheduleInfo& info);

private:
    kmonitor_adapter::MetricPtr _candidateRoleCountMetric;
    kmonitor_adapter::MetricPtr _assignedRoleCountMetric;
    kmonitor_adapter::MetricPtr _waitScheduleRoleCountMetric;

    kmonitor_adapter::MetricPtr _totalAgentNodeCountMetric;  // config node count
    kmonitor_adapter::MetricPtr _assignAgentNodeCountMetric; // in plan agent count
    kmonitor_adapter::MetricPtr _idleAgentNodeCountMetric;   // agent has no target role
    kmonitor_adapter::MetricPtr _busyAgentNodeCountMetric;   // agent has target role
    kmonitor_adapter::MetricPtr _readyAgentNodeCountMetric;  // agent identifier is not empty

    bool _isGlobalAgent;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(AgentMetricReporter);

}} // namespace build_service::admin
