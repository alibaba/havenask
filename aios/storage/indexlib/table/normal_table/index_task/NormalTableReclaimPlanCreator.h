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
#include "indexlib/framework/MetricsWrapper.h"
#include "indexlib/framework/index_task/IndexTaskPlan.h"
#include "indexlib/table/index_task/SimpleIndexTaskPlanCreator.h"

namespace indexlibv2::framework {
class IndexOperationDescription;
class Version;
class IndexTaskContext;
class Segment;
} // namespace indexlibv2::framework
namespace indexlibv2::table {

class NormalTableReclaimPlanCreator : public SimpleIndexTaskPlanCreator
{
public:
    NormalTableReclaimPlanCreator(const std::string& taskName, const std::string& taskTraceId,
                                  const std::map<std::string, std::string>& params);
    ~NormalTableReclaimPlanCreator();

public:
    std::pair<Status, std::unique_ptr<framework::IndexTaskPlan>>
    CreateTaskPlan(const framework::IndexTaskContext* taskContext) override;

    static Status AppendDefaultConfigIfNecessary(const framework::IndexTaskContext* context,
                                                 std::vector<config::IndexTaskConfig>* configs);

private:
    std::string ConstructLogTaskType() const override;
    std::string ConstructLogTaskId(const framework::IndexTaskContext* taskContext,
                                   const framework::Version& targetVersion) const override;

    std::tuple<Status, std::vector<framework::IndexOperationDescription>, framework::Version>
    CreateOperationDescriptions(const framework::IndexTaskContext* taskContext);

    std::pair<Status, bool> IsSegmentHasValidPatch(const std::shared_ptr<framework::Segment>& segment,
                                                   const framework::IndexTaskContext* taskContext);

public:
    static const std::string TASK_TYPE;
    static const std::string DEFAULT_TASK_NAME;
    static const std::string DEFAULT_TASK_PERIOD;

private:
    INDEXLIB_FM_DECLARE_METRIC(TTLReclaimSegmentCount);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
