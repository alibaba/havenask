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
#include "build_service/admin/ScheduleMetricReporter.h"

#include <algorithm>
#include <cstddef>
#include <utility>

#include "alog/Logger.h"
#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "beeper/beeper.h"
#include "build_service/common/BeeperCollectorDefine.h"
#include "build_service/proto/ProtoUtil.h"
#include "build_service/util/ErrorLogCollector.h"
#include "build_service/util/Monitor.h"
#include "kmonitor/client/MetricLevel.h"

using namespace std;
using namespace hippo;
using namespace autil;
using namespace build_service::proto;
using namespace build_service::util;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, ScheduleMetricReporter);

ScheduleMetricReporter::SlotRecord::SlotRecord()
    : startLatency(-1)
    , declareTime(-1)
    , allocatedTime(-1)
    , installedTime(-1)
    , startedTime(-1)
    , restartCount(0)
    , totalRestartCount(0)
    , beeperReportTs(0)
    , roleType(proto::RoleType::ROLE_UNKNOWN)
{
}

ScheduleMetricReporter::SlotRecord::SlotRecord(proto::RoleType rt)
    : startLatency(-1)
    , declareTime(-1)
    , allocatedTime(-1)
    , installedTime(-1)
    , startedTime(-1)
    , restartCount(0)
    , totalRestartCount(0)
    , beeperReportTs(0)
    , roleType(rt)
{
}

void ScheduleMetricReporter::SlotRecord::reset()
{
    startLatency = -1;
    declareTime = -1;
    allocatedTime = -1;
    installedTime = -1;
    startedTime = -1;
    address.clear();
}

string ScheduleMetricReporter::METRIC_TAG_PHASE = "phase";
string ScheduleMetricReporter::METRIC_TAG_ROLETYPE = "roleType";
string ScheduleMetricReporter::METRIC_TAG_ROLENAME = "roleName";
string ScheduleMetricReporter::METRIC_TAG_GENERATION = "generation";
string ScheduleMetricReporter::METRIC_TAG_APPNAME = "appName";
string ScheduleMetricReporter::METRIC_TAG_DATATABLE = "dataTable";
string ScheduleMetricReporter::METRIC_TAG_COUNT_TYPE = "countType";
string ScheduleMetricReporter::METRIC_TAG_SLOT_IP = "slotIp";
char ScheduleMetricReporter::METRIC_TAG_INVAILD_CHAR = ':';
char ScheduleMetricReporter::METRIC_TAG_ESCAPE_CHAR = '-';

// restarting over 5 times will report event to beeper
int64_t ScheduleMetricReporter::DEFAULT_RESTART_THRESHOLD_TO_REPORT = 5;
int64_t ScheduleMetricReporter::DEFAULT_BEEPER_REPORT_INTERVAL = 300;

ScheduleMetricReporter::ScheduleMetricReporter()
{
    _restartThresholdToReport =
        EnvUtil::getEnv(BS_ENV_ADMIN_RESTART_THRESHOLD_TO_REPORT, DEFAULT_RESTART_THRESHOLD_TO_REPORT);
}

ScheduleMetricReporter::~ScheduleMetricReporter() {}

void ScheduleMetricReporter::init(proto::BuildId buildId, kmonitor_adapter::Monitor* monitor)
{
    if (monitor == NULL) {
        BS_LOG(INFO, "init ScheduleMetricReporter failed");
        return;
    }
    BS_LOG(INFO, "init ScheduleMetricReporter");
    _generationId = StringUtil::toString(buildId.generationid());
    _appName = buildId.appname();
    _dataTable = buildId.datatable();

    _tags.AddTag(ScheduleMetricReporter::METRIC_TAG_GENERATION, _generationId);
    _tags.AddTag(ScheduleMetricReporter::METRIC_TAG_APPNAME, _appName);
    _tags.AddTag(ScheduleMetricReporter::METRIC_TAG_DATATABLE, _dataTable);

    _countMetric = monitor->registerCounterMetric("slot.count", kmonitor::FATAL);
    // this metric will report all the time during each phase
    // usefully to calculate slot count for each phase
    _slotLatencyMetric = monitor->registerGaugePercentileMetric("slot.latency", kmonitor::FATAL);
    // summarized means only report once in each phase
    _summarizedLatencyMetric = monitor->registerGaugePercentileMetric("slot.summarizedLatency", kmonitor::FATAL);

    _restartMetric = monitor->registerGaugeMetric("slot.restartCount", kmonitor::FATAL);
    _restartQpsMetric = monitor->registerGaugeMetric("slot.restartQps", kmonitor::FATAL);

    _beeperTags.reset(new beeper::EventTags);
    ProtoUtil::buildIdToBeeperTags(buildId, *_beeperTags);
}

bool ScheduleMetricReporter::updateCount(const GroupRole& groupRole, const RoleSlotInfosPtr& roleSlots)
{
    const string& roleName = groupRole.roleName;
    RoleType roleType = groupRole.type;

    int64_t curTs = TimeUtility::currentTime();
    _tags.DelTag(ScheduleMetricReporter::METRIC_TAG_ROLETYPE);
    _tags.AddTag(ScheduleMetricReporter::METRIC_TAG_ROLETYPE, ProtoUtil::toRoleString(roleType));
    _tags.DelTag(ScheduleMetricReporter::METRIC_TAG_ROLENAME);
    _tags.AddTag(ScheduleMetricReporter::METRIC_TAG_ROLENAME, roleName);

    _tags.DelTag(ScheduleMetricReporter::METRIC_TAG_SLOT_IP);
    _tags.DelTag(ScheduleMetricReporter::METRIC_TAG_PHASE);
    _tags.DelTag(ScheduleMetricReporter::METRIC_TAG_COUNT_TYPE);

    if (_slotRecord.find(roleName) == _slotRecord.end()) {
        SlotRecord record(roleType);
        record.declareTime = curTs;
        record.roleName = roleName;
        _slotRecord.insert(make_pair(roleName, record));
    }
    auto slotRecord = _slotRecord.find(roleName);
    // report total time before slot is started
    if (slotRecord->second.startedTime == -1) {
        _tags.DelTag(ScheduleMetricReporter::METRIC_TAG_COUNT_TYPE);
        _tags.DelTag(ScheduleMetricReporter::METRIC_TAG_PHASE);
        _tags.AddTag(ScheduleMetricReporter::METRIC_TAG_PHASE, "total");
        REPORT_KMONITOR_METRIC2(_slotLatencyMetric, _tags, curTs - slotRecord->second.declareTime);

        int64_t now = TimeUtility::currentTimeInSeconds();
        if ((curTs - slotRecord->second.declareTime) > 900 * 1000 * 1000 // 15 min
            && now - slotRecord->second.beeperReportTs > DEFAULT_BEEPER_REPORT_INTERVAL) {
            BEEPER_FORMAT_REPORT(GENERATION_ERROR_COLLECTOR_NAME, *_beeperTags,
                                 "roleName [%s] wait to be running status over 15 min.",
                                 slotRecord->second.roleName.c_str());
            slotRecord->second.beeperReportTs = now;
        }
    }

    if (!roleSlots) {
        if (slotRecord->second.startedTime != -1 || !slotRecord->second.address.empty()) {
            // for restarting or migrating node
            // for node that has never started
            slotRecord->second.reset();
            slotRecord->second.declareTime = curTs;
            return false;
        }
        _tags.DelTag(ScheduleMetricReporter::METRIC_TAG_PHASE);
        _tags.AddTag(ScheduleMetricReporter::METRIC_TAG_PHASE, "allocate");
        REPORT_KMONITOR_METRIC2(_slotLatencyMetric, _tags, curTs - slotRecord->second.declareTime);
        return false;
    }

    auto it = roleSlots->find(roleName);
    if (it == roleSlots->end() || it->second.size() < 1) {
        if (slotRecord->second.startedTime != -1 || !slotRecord->second.address.empty()) {
            // for restarting or migrating node
            // for node that has never started
            slotRecord->second.reset();
            slotRecord->second.declareTime = curTs;
            return false;
        }
        _tags.DelTag(ScheduleMetricReporter::METRIC_TAG_PHASE);
        _tags.AddTag(ScheduleMetricReporter::METRIC_TAG_PHASE, "allocate");
        REPORT_KMONITOR_METRIC2(_slotLatencyMetric, _tags, curTs - slotRecord->second.declareTime);
        return false;
    }
    if (it->second.size() == 0) {
        return false;
    }

    size_t slotId = getLatestSlotId(it->second);
    SlotInfo& slotInfo = it->second[slotId];

    string address = slotInfo.slotId.slaveAddress;
    autil::StringUtil::replace(address, ScheduleMetricReporter::METRIC_TAG_INVAILD_CHAR,
                               ScheduleMetricReporter::METRIC_TAG_ESCAPE_CHAR);
    _tags.AddTag(ScheduleMetricReporter::METRIC_TAG_SLOT_IP, address);
    if (slotRecord->second.address.empty()) {
        slotRecord->second.address = address;
    }
    if (slotRecord->second.address != address) {
        // deal with restarting slot or new slot with same rolename
        slotRecord->second.reset();
        slotRecord->second.declareTime = curTs;
        slotRecord->second.address = address;
    }

    // allocated successfully
    if (slotRecord->second.allocatedTime == -1) {
        slotRecord->second.allocatedTime = curTs;
        slotRecord->second.address = address;
        _tags.DelTag(ScheduleMetricReporter::METRIC_TAG_PHASE);
        _tags.AddTag(ScheduleMetricReporter::METRIC_TAG_PHASE, "allocate");
        REPORT_KMONITOR_METRIC2(_summarizedLatencyMetric, _tags, curTs - slotRecord->second.declareTime);
        if (curTs - slotRecord->second.declareTime > 300 * 1000 * 1000) {
            BEEPER_FORMAT_REPORT(GENERATION_ERROR_COLLECTOR_NAME, *_beeperTags,
                                 "allocate roleName [%s] cost [%ld] over 5 min", slotRecord->second.roleName.c_str(),
                                 (curTs - slotRecord->second.declareTime) / (1000 * 1000));
        }
    }
    // install phase
    if (!handleInstallStatus(slotInfo, curTs, slotRecord->second)) {
        return false;
    }

    if (slotInfo.processStatus.size() <= 0) {
        // TODO add log
        return false;
    }
    // running phase
    return handleProcessStatus(slotInfo, curTs, slotRecord->second);
}

bool ScheduleMetricReporter::handleInstallStatus(const SlotInfo& slotInfo, const int64_t& curTs, SlotRecord& slotRecord)
{
    if (slotInfo.packageStatus.status == PackageStatus::Status::IS_UNKNOWN ||
        slotInfo.packageStatus.status == PackageStatus::Status::IS_WAITING ||
        slotInfo.packageStatus.status == PackageStatus::Status::IS_INSTALLING) {
        _tags.DelTag(ScheduleMetricReporter::METRIC_TAG_PHASE);
        _tags.AddTag(ScheduleMetricReporter::METRIC_TAG_PHASE, "install");
        REPORT_KMONITOR_METRIC2(_slotLatencyMetric, _tags, curTs - std::max(slotRecord.allocatedTime, (int64_t)0));
        return false;
    }

    if (slotRecord.installedTime == -1) {
        // install successfully
        slotRecord.installedTime = curTs;
        _tags.DelTag(ScheduleMetricReporter::METRIC_TAG_PHASE);
        _tags.AddTag(ScheduleMetricReporter::METRIC_TAG_PHASE, "install");
        REPORT_KMONITOR_METRIC2(_summarizedLatencyMetric, _tags,
                                curTs - std::max(slotRecord.allocatedTime, (int64_t)0));
        return true;
    }

    if (slotInfo.packageStatus.status == PackageStatus::Status::IS_FAILED) {
        _tags.DelTag(ScheduleMetricReporter::METRIC_TAG_PHASE);
        _tags.DelTag(ScheduleMetricReporter::METRIC_TAG_COUNT_TYPE);
        _tags.AddTag(ScheduleMetricReporter::METRIC_TAG_COUNT_TYPE, "installFailed");
        REPORT_KMONITOR_METRIC2(_countMetric, _tags, 1);

        int64_t now = TimeUtility::currentTimeInSeconds();
        if (now - slotRecord.beeperReportTs > DEFAULT_BEEPER_REPORT_INTERVAL) {
            string address = slotInfo.slotId.slaveAddress;
            BEEPER_FORMAT_REPORT(GENERATION_ERROR_COLLECTOR_NAME, *_beeperTags,
                                 "roleName[%s] address [%s] install failed.", slotRecord.roleName.c_str(),
                                 address.c_str());
            slotRecord.beeperReportTs = now;
        }
        return false;
    }
    return true;
}

bool ScheduleMetricReporter::handleProcessStatus(const SlotInfo& slotInfo, const int64_t& curTs, SlotRecord& slotRecord)
{
    if (slotInfo.processStatus[0].status == ProcessStatus::Status::PS_FAILED) {
        _tags.DelTag(ScheduleMetricReporter::METRIC_TAG_PHASE);
        _tags.DelTag(ScheduleMetricReporter::METRIC_TAG_COUNT_TYPE);
        _tags.AddTag(ScheduleMetricReporter::METRIC_TAG_COUNT_TYPE, "psFailed");
        REPORT_KMONITOR_METRIC2(_countMetric, _tags, 1);

        int64_t now = TimeUtility::currentTimeInSeconds();
        if (now - slotRecord.beeperReportTs > DEFAULT_BEEPER_REPORT_INTERVAL) {
            string address = slotInfo.slotId.slaveAddress;
            BEEPER_FORMAT_REPORT(GENERATION_ERROR_COLLECTOR_NAME, *_beeperTags, "roleName[%s] address [%s] ps failed.",
                                 slotRecord.roleName.c_str(), address.c_str());
            slotRecord.beeperReportTs = now;
        }
        return false;
    }
    if (slotInfo.processStatus[0].status == ProcessStatus::Status::PS_UNKNOWN) {
        _tags.DelTag(ScheduleMetricReporter::METRIC_TAG_PHASE);
        _tags.AddTag(ScheduleMetricReporter::METRIC_TAG_PHASE, "start");
        REPORT_KMONITOR_METRIC2(_slotLatencyMetric, _tags, curTs - std::max(slotRecord.installedTime, (int64_t)0));
        return false;
    }
    if (slotInfo.processStatus[0].status == ProcessStatus::Status::PS_RESTARTING) {
        // only restart in the same slot will trigger the following code
        _tags.DelTag(ScheduleMetricReporter::METRIC_TAG_PHASE);
        _tags.DelTag(ScheduleMetricReporter::METRIC_TAG_COUNT_TYPE);
        _tags.AddTag(ScheduleMetricReporter::METRIC_TAG_COUNT_TYPE, "psRestarting");
        REPORT_KMONITOR_METRIC2(_countMetric, _tags, 1);

        _tags.DelTag(ScheduleMetricReporter::METRIC_TAG_PHASE);
        _tags.DelTag(ScheduleMetricReporter::METRIC_TAG_COUNT_TYPE);
        if (slotRecord.restartCount != slotInfo.processStatus[0].restartCount) {
            int64_t restartDelta = slotRecord.restartCount < slotInfo.processStatus[0].restartCount
                                       ? slotInfo.processStatus[0].restartCount - slotRecord.restartCount
                                       : slotInfo.processStatus[0].restartCount;
            slotRecord.totalRestartCount += restartDelta;
            slotRecord.restartCount = slotInfo.processStatus[0].restartCount;
            REPORT_KMONITOR_METRIC2(_restartQpsMetric, _tags, restartDelta);
            REPORT_KMONITOR_METRIC2(_restartMetric, _tags, slotRecord.totalRestartCount);
            if (slotRecord.totalRestartCount > _restartThresholdToReport) {
                int64_t now = TimeUtility::currentTimeInSeconds();
                if (now - slotRecord.beeperReportTs > DEFAULT_BEEPER_REPORT_INTERVAL) {
                    BEEPER_FORMAT_REPORT(GENERATION_ERROR_COLLECTOR_NAME, *_beeperTags,
                                         "restart node: roleName[%s] totalRestartCnt[%ld]", slotRecord.roleName.c_str(),
                                         slotRecord.totalRestartCount);
                    slotRecord.beeperReportTs = now;
                }
            }
        }

        return false;
    }
    if (slotInfo.processStatus[0].status == ProcessStatus::Status::PS_RUNNING) {
        _tags.DelTag(ScheduleMetricReporter::METRIC_TAG_COUNT_TYPE);
        _tags.AddTag(ScheduleMetricReporter::METRIC_TAG_COUNT_TYPE, "psRunning");
        REPORT_KMONITOR_METRIC2(_countMetric, _tags, 1);

        if (slotRecord.startedTime == -1) {
            slotRecord.startedTime = curTs;
            _tags.DelTag(ScheduleMetricReporter::METRIC_TAG_COUNT_TYPE);
            _tags.DelTag(ScheduleMetricReporter::METRIC_TAG_PHASE);
            _tags.AddTag(ScheduleMetricReporter::METRIC_TAG_PHASE, "start");
            REPORT_KMONITOR_METRIC2(_summarizedLatencyMetric, _tags,
                                    slotRecord.startedTime - std::max(slotRecord.installedTime, (int64_t)0));
            REPORT_KMONITOR_METRIC2(_slotLatencyMetric, _tags,
                                    slotRecord.startedTime - std::max(slotRecord.installedTime, (int64_t)0));

            _tags.DelTag(ScheduleMetricReporter::METRIC_TAG_PHASE);
            _tags.AddTag(ScheduleMetricReporter::METRIC_TAG_PHASE, "total");
            REPORT_KMONITOR_METRIC2(_slotLatencyMetric, _tags, slotRecord.startedTime - slotRecord.declareTime);
            REPORT_KMONITOR_METRIC2(_summarizedLatencyMetric, _tags, slotRecord.startedTime - slotRecord.declareTime);
        }
    }

    return true;
}

size_t ScheduleMetricReporter::getLatestSlotId(std::vector<hippo::SlotInfo>& slotInfos)
{
    int64_t declareTime = slotInfos[0].slotId.declareTime;
    size_t idx = 0;
    for (size_t i = 1; i < slotInfos.size(); ++i) {
        if (slotInfos[i].slotId.declareTime > declareTime) {
            declareTime = slotInfos[i].slotId.declareTime;
            idx = i;
        }
    }
    return idx;
}

void ScheduleMetricReporter::updateMetrics(const shared_ptr<GroupRolePairs>& candidates,
                                           const RoleSlotInfosPtr& roleSlots)
{
    auto iter = _slotRecord.begin();
    while (iter != _slotRecord.end()) {
        if (candidates->find(iter->first) == candidates->end()) {
            _slotRecord.erase(iter++);
            continue;
        }
        ++iter;
    }

    for (auto it = candidates->begin(); it != candidates->end(); ++it) {
        updateCount(it->second, roleSlots);
    }
}

}} // namespace build_service::admin
