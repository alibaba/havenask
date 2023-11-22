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

#include <map>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "beeper/common/common_type.h"
#include "build_service/admin/MetaTagAppender.h"
#include "build_service/common_define.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/util/Log.h"
#include "hippo/DriverCommon.h"
#include "kmonitor/client/core/MetricsTags.h"
#include "kmonitor_adapter/Metric.h"
#include "kmonitor_adapter/Monitor.h"
#include "master_framework/ScheduleUnit.h"

namespace build_service { namespace admin {

class ScheduleMetricReporter
{
public:
    ScheduleMetricReporter();
    ~ScheduleMetricReporter();

public:
    typedef std::shared_ptr<RoleSlotInfos> RoleSlotInfosPtr;

private:
    ScheduleMetricReporter(const ScheduleMetricReporter&);
    ScheduleMetricReporter& operator=(const ScheduleMetricReporter&);

public:
    void init(proto::BuildId buildId, kmonitor_adapter::Monitor* globalMonitor);
    void updateMetrics(const std::shared_ptr<GroupRolePairs>& candidates, const RoleSlotInfosPtr& slotInfos);

private:
    struct SlotRecord {
        int64_t startLatency; // from declare to running
        int64_t declareTime;
        int64_t allocatedTime;
        int64_t installedTime;
        int64_t startedTime;
        int64_t restartCount;
        int64_t totalRestartCount;
        int64_t beeperReportTs;

        std::string address;
        proto::RoleType roleType;
        std::string roleName;
        SlotRecord();
        SlotRecord(proto::RoleType roleType);
        void reset();
    };

private:
    bool updateCount(const GroupRole& groupRole, const RoleSlotInfosPtr& slotInfos);
    size_t getLatestSlotId(std::vector<hippo::SlotInfo>& slotInfos);

    bool handleInstallStatus(const hippo::SlotInfo& slotInfo, const int64_t& curTs, SlotRecord& slotRecord);
    bool handleProcessStatus(const hippo::SlotInfo& slotInfo, const int64_t& curTs, SlotRecord& slotRecord);

private:
    kmonitor_adapter::Monitor* _monitor;
    std::string _generationId;
    std::string _appName;
    std::string _dataTable;
    std::map<std::string, SlotRecord> _slotRecord;

    kmonitor::MetricsTags _tags;
    kmonitor_adapter::MetricPtr _countMetric;
    kmonitor_adapter::MetricPtr _restartMetric;
    kmonitor_adapter::MetricPtr _restartQpsMetric;
    kmonitor_adapter::MetricPtr _slotLatencyMetric;
    kmonitor_adapter::MetricPtr _summarizedLatencyMetric;

    beeper::EventTagsPtr _beeperTags;
    int64_t _restartThresholdToReport;

private:
    static std::string METRIC_TAG_PHASE;
    static std::string METRIC_TAG_ROLETYPE;
    static std::string METRIC_TAG_ROLENAME;
    static std::string METRIC_TAG_GENERATION;
    static std::string METRIC_TAG_APPNAME;
    static std::string METRIC_TAG_DATATABLE;
    static std::string METRIC_TAG_COUNT_TYPE;
    static std::string METRIC_TAG_SLOT_IP;

    static char METRIC_TAG_INVAILD_CHAR;
    static char METRIC_TAG_ESCAPE_CHAR;

    static int64_t DEFAULT_RESTART_THRESHOLD_TO_REPORT;
    static int64_t DEFAULT_BEEPER_REPORT_INTERVAL;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ScheduleMetricReporter);

}} // namespace build_service::admin
