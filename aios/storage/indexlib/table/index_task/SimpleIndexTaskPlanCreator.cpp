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
#include "indexlib/table/index_task/SimpleIndexTaskPlanCreator.h"

#include "autil/StringTokenizer.h"
#include "indexlib/config/IndexTaskConfig.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/framework/index_task/IndexTaskContext.h"
#include "indexlib/framework/index_task/IndexTaskPlan.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, SimpleIndexTaskPlanCreator);

SimpleIndexTaskPlanCreator::SimpleIndexTaskPlanCreator(const std::string& taskName,
                                                       const std::map<std::string, std::string>& params)
    : _taskName(taskName)
    , _params(params)
{
}

SimpleIndexTaskPlanCreator::~SimpleIndexTaskPlanCreator() {}

std::pair<Status, bool>
SimpleIndexTaskPlanCreator::NeedTriggerTask(const config::IndexTaskConfig& taskConfig,
                                            const framework::IndexTaskContext* taskContext) const
{
    if (_taskName != taskConfig.GetTaskName()) {
        AUTIL_LOG(ERROR, "task name [%s] not match with the name [%s] in task config", _taskName.c_str(),
                  taskConfig.GetTaskName().c_str());
        return std::make_pair(Status::Unknown(), false);
    }

    auto tabletData = taskContext->GetTabletData();
    if (!tabletData) {
        return std::make_pair(Status::Unknown("tabletData is nullptr"), false);
    }
    const auto& history = tabletData->GetOnDiskVersion().GetIndexTaskHistory();
    int64_t lastTaskTimestamp = history.GetOldestTaskLogTimestamp();
    if (!history.FindTaskConfigEffectiveTimestamp(taskConfig.GetTaskName(), taskConfig.GetTaskType(),
                                                  lastTaskTimestamp)) {
        AUTIL_LOG(ERROR, "find task config effective timestamp fail, taskType [%s], taskName [%s]",
                  taskConfig.GetTaskType().c_str(), _taskName.c_str());
    }
    const auto& logItems = history.GetTaskLogs(ConstructLogTaskType());
    if (!logItems.empty()) {
        lastTaskTimestamp = logItems[0]->GetTriggerTimestampInSecond();
    }

    auto trigger = taskConfig.GetTrigger();
    auto triggerType = trigger.GetTriggerType();
    switch (triggerType) {
    case config::Trigger::TriggerType::AUTO:
        return {Status::OK(), true};
    case config::Trigger::TriggerType::MANUAL:
        return {Status::OK(), false};
    case config::Trigger::TriggerType::DATE_TIME:
        assert(trigger.GetTriggerDayTime() != std::nullopt);
        return {Status::OK(), NeedDaytimeTrigger(trigger.GetTriggerDayTime().value(), lastTaskTimestamp)};
    case config::Trigger::TriggerType::PERIOD:
        assert(trigger.GetTriggerPeriod() != std::nullopt);
        return {Status::OK(), NeedPeriodTrigger(trigger.GetTriggerPeriod().value(), lastTaskTimestamp)};
    case config::Trigger::TriggerType::ERROR:
    default:
        assert(false);
        return std::make_pair(Status::InvalidArgs(""), false);
    }
}

bool SimpleIndexTaskPlanCreator::NeedDaytimeTrigger(const struct tm& triggerTime, int64_t lastTaskTsInSec) const
{
    struct tm nextTriggerTime;
    localtime_r(&lastTaskTsInSec, &nextTriggerTime);
    nextTriggerTime.tm_sec = 0;
    nextTriggerTime.tm_min = triggerTime.tm_min;
    nextTriggerTime.tm_hour = triggerTime.tm_hour;
    time_t nextTriggerTs = mktime(&nextTriggerTime);
    if (nextTriggerTs <= lastTaskTsInSec) {
        nextTriggerTs += 3600 * 24;
    }
    time_t nowTime = time(NULL);
    return nowTime >= nextTriggerTs;
}

bool SimpleIndexTaskPlanCreator::NeedPeriodTrigger(int64_t period, int64_t lastTaskTsInSec) const
{
    assert(period > 0);
    return autil::TimeUtility::currentTimeInSeconds() >= (lastTaskTsInSec + period);
}

Status SimpleIndexTaskPlanCreator::CommitTaskLogToVersion(const framework::IndexTaskContext* taskContext,
                                                          framework::Version& targetVersion)
{
    std::string logType = ConstructLogTaskType();
    std::string taskId = ConstructLogTaskId(taskContext, targetVersion);
    const auto& baseVersion = taskContext->GetTabletData()->GetOnDiskVersion();
    auto taskLog = std::make_shared<framework::IndexTaskLog>(
        taskId, baseVersion.GetVersionId(), targetVersion.GetVersionId(), autil::TimeUtility::currentTimeInSeconds(),
        taskContext->GetAllParameters());
    targetVersion.AddIndexTaskLog(logType, taskLog);
    return Status::OK();
}

} // namespace indexlibv2::table
