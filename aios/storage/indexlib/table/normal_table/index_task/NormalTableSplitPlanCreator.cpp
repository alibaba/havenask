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
#include "indexlib/table/normal_table/index_task/NormalTableSplitPlanCreator.h"

#include "indexlib/config/ITabletSchema.h"
#include "indexlib/config/IndexTaskConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/table/normal_table/Common.h"
#include "indexlib/table/normal_table/config/SegmentGroupConfig.h"
#include "indexlib/table/normal_table/index_task/merger/SplitStrategy.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, NormalTableSplitPlanCreator);

const std::string NormalTableSplitPlanCreator::DEFAULT_TASK_NAME = "__default_split__";
const std::string NormalTableSplitPlanCreator::TASK_TYPE = "split";
const std::string NormalTableSplitPlanCreator::DEFAULT_TASK_PERIOD = "daytime=1:00";

NormalTableSplitPlanCreator::NormalTableSplitPlanCreator(const std::string& taskName, const std::string& taskTraceId,
                                                         const std::map<std::string, std::string>& params)
    : NormalTableCompactPlanCreator(taskName, taskTraceId, params)
{
}

NormalTableSplitPlanCreator::~NormalTableSplitPlanCreator() {}

std::pair<std::string, std::unique_ptr<MergeStrategy>>
NormalTableSplitPlanCreator::CreateCompactStrategy(const std::string& mergeStrategyName, bool isOptimizeMerge)
{
    return {NORMAL_TABLE_SPLIT_TYPE, std::make_unique<SplitStrategy>()};
}

static std::pair<Status, bool> IsGroupEnabled(const std::shared_ptr<config::ITabletSchema>& schema)
{
    auto [status, segmentGroupConfig] =
        schema->GetRuntimeSettings().GetValue<SegmentGroupConfig>(NORMAL_TABLE_GROUP_CONFIG_KEY);
    if (!status.IsOK() && !status.IsNotFound()) {
        return {status, false};
    }
    return {Status::OK(), segmentGroupConfig.IsGroupEnabled()};
}

std::pair<Status, bool>
NormalTableSplitPlanCreator::NeedTriggerTask(const config::IndexTaskConfig& taskConfig,
                                             const framework::IndexTaskContext* taskContext) const
{
    auto [status, needTrigger] = SimpleIndexTaskPlanCreator::NeedTriggerTask(taskConfig, taskContext);
    RETURN2_IF_STATUS_ERROR(status, false, "check need trigger task failed");
    if (!needTrigger) {
        AUTIL_LOG(INFO, "no need to split");
        return {Status::OK(), false};
    }
    // TODO(xiaohao.yxh) add test
    auto [status2, groupEnabled] = IsGroupEnabled(taskContext->GetTabletSchema());
    RETURN2_IF_STATUS_ERROR(status2, false, "check group enabled failed");
    if (!groupEnabled) {
        AUTIL_LOG(INFO, "group not enabled, ignore split task");
        return {Status::OK(), false};
    }

    for (auto& segment : taskContext->GetTabletData()->CreateSlice()) {
        if (framework::Segment::IsMergedSegmentId(segment->GetSegmentId())) {
            return {Status::OK(), true};
        }
    }
    AUTIL_LOG(INFO, "no need to split because no merged segment");
    return {Status::OK(), false};
}

Status NormalTableSplitPlanCreator::AppendDefaultConfigIfNecessary(const framework::IndexTaskContext* context,
                                                                   std::vector<config::IndexTaskConfig>* configs)
{
    auto [status, groupEnabled] = IsGroupEnabled(context->GetTabletSchema());
    RETURN_IF_STATUS_ERROR(status, "check group enabled failed");
    if (groupEnabled) {
        for (const auto& config : *configs) {
            if (config.GetTaskType() == TASK_TYPE) {
                return Status::OK();
            }
        }
        auto defaultSplitConfig = config::IndexTaskConfig(DEFAULT_TASK_NAME, TASK_TYPE, DEFAULT_TASK_PERIOD);
        configs->push_back(defaultSplitConfig);
        AUTIL_LOG(INFO, "append default split task config name [%s], period [%s] to task config",
                  DEFAULT_TASK_NAME.c_str(), DEFAULT_TASK_PERIOD.c_str());
        auto tabletData = context->GetTabletData();
        if (tabletData) {
            tabletData->DeclareTaskConfig(defaultSplitConfig.GetTaskName(), defaultSplitConfig.GetTaskType());
        }
    }
    return Status::OK();
}

} // namespace indexlibv2::table
