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
#include "indexlib/framework/Version.h"
#include "indexlib/framework/index_task/IIndexTaskPlanCreator.h"

namespace indexlibv2::config {
class IndexTaskConfig;
};

namespace indexlibv2::table {

class SimpleIndexTaskPlanCreator : public framework::IIndexTaskPlanCreator
{
public:
    SimpleIndexTaskPlanCreator(const std::string& taskName, const std::string& taskTraceId,
                               const std::map<std::string, std::string>& params);
    ~SimpleIndexTaskPlanCreator();

    virtual std::string ConstructLogTaskType() const = 0;
    virtual std::string ConstructLogTaskId(const framework::IndexTaskContext* taskContext,
                                           const framework::Version& targetVersion) const = 0;

    virtual std::pair<Status, bool> NeedTriggerTask(const config::IndexTaskConfig& taskConfig,
                                                    const framework::IndexTaskContext* taskContext) const;

protected:
    /* CommitTaskLogToVersion need to be called in override function CreateTaskPlan */
    /* so that task log will exist in target version  */
    Status CommitTaskLogToVersion(const framework::IndexTaskContext* taskContext, framework::Version& targetVersion);
    bool NeedDaytimeTrigger(const struct tm& triggerTime, const int64_t lastTaskTsInSec) const;
    bool NeedPeriodTrigger(int64_t period, int64_t lastTaskTsInSec) const;

protected:
    std::string _taskName;
    std::string _taskTraceId;
    std::map<std::string, std::string> _params;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
