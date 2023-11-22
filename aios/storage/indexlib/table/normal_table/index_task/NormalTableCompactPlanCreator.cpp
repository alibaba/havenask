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
#include "indexlib/table/normal_table/index_task/NormalTableCompactPlanCreator.h"

#include <set>

#include "autil/legacy/jsonizable.h"
#include "indexlib/base/Status.h"
#include "indexlib/config/IndexTaskConfig.h"
#include "indexlib/config/MergeConfig.h"
#include "indexlib/config/OfflineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/file_system/JsonUtil.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/SegmentInfo.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/framework/Version.h"
#include "indexlib/framework/index_task/IndexTaskContext.h"
#include "indexlib/framework/index_task/IndexTaskPlan.h"
#include "indexlib/framework/index_task/IndexTaskResource.h"
#include "indexlib/framework/index_task/IndexTaskResourceManager.h"
#include "indexlib/table/BuiltinDefine.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/table/index_task/merger/MergeStrategy.h"
#include "indexlib/table/index_task/merger/MergeStrategyDefine.h"
#include "indexlib/table/index_task/merger/SpecificSegmentsMergeStrategy.h"
#include "indexlib/table/normal_table/NormalSchemaResolver.h"
#include "indexlib/table/normal_table/index_task/NormalTableMergeDescriptionCreator.h"
#include "indexlib/table/normal_table/index_task/PatchedDeletionMapLoader.h"
#include "indexlib/table/normal_table/index_task/ReclaimMap.h"
#include "indexlib/table/normal_table/index_task/SortedReclaimMap.h"
#include "indexlib/table/normal_table/index_task/merger/AdaptiveMergeStrategy.h"
#include "indexlib/table/normal_table/index_task/merger/BalanceTreeMergeStrategy.h"
#include "indexlib/table/normal_table/index_task/merger/CombinedMergeStrategy.h"
#include "indexlib/table/normal_table/index_task/merger/NormalTabletOptimizeMergeStrategy.h"
#include "indexlib/table/normal_table/index_task/merger/PriorityQueueMergeStrategy.h"

using namespace std;

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, NormalTableCompactPlanCreator);

const std::string NormalTableCompactPlanCreator::TASK_TYPE = MERGE_TASK_TYPE;

NormalTableCompactPlanCreator::NormalTableCompactPlanCreator(const std::string& taskName,
                                                             const std::string& taskTraceId,
                                                             const std::map<std::string, std::string>& params)
    : SimpleIndexTaskPlanCreator(taskName, taskTraceId, params)
{
}

NormalTableCompactPlanCreator::~NormalTableCompactPlanCreator() {}

std::pair<Status, std::unique_ptr<framework::IndexTaskPlan>>
NormalTableCompactPlanCreator::CreateTaskPlan(const framework::IndexTaskContext* taskContext)
{
    auto mergeConfig = taskContext->GetMergeConfig();
    bool optimize = false;
    if (!taskContext->GetParameter(IS_OPTIMIZE_MERGE, optimize)) {
        optimize = false;
    }
    auto tabletSchema = taskContext->GetTabletSchema();
    std::string indexType = index::PRIMARY_KEY_INDEX_TYPE_STR;
    auto pkConfigs = tabletSchema->GetIndexConfigs(indexType);
    if (tabletSchema->GetTableType() == indexlib::table::TABLE_TYPE_NORMAL && pkConfigs.size() > 0) {
        std::vector<std::pair<segmentid_t, std::shared_ptr<index::DeletionMapDiskIndexer>>> patchedIndexers;
        RETURN2_IF_STATUS_ERROR(
            PatchedDeletionMapLoader::GetPatchedDeletionMapDiskIndexers(taskContext->GetTabletData(),
                                                                        /*patchDir*/ nullptr, &patchedIndexers),
            nullptr, "get patch deletion map indexer from tablet failed");
    }
    auto [compactionType, mergeStrategy] = CreateCompactStrategy(mergeConfig.GetMergeStrategyStr(), optimize);
    if (!mergeStrategy) {
        RETURN2_IF_STATUS_ERROR(Status::ConfigError(), nullptr, "create merge strategy failed");
    }
    auto mergePlanPair = mergeStrategy->CreateMergePlan(taskContext);
    if (!mergePlanPair.first.IsOK()) {
        return std::make_pair(mergePlanPair.first, nullptr);
    }
    auto mergePlan = mergePlanPair.second;
    if (mergePlan->Size() == 0) {
        // no merge plan create, restore tablet options
        return std::make_pair(Status::OK(), nullptr);
    }

    // TODO(yijie.zhang): for truncate, all not merged segment should do merge
    // TODO(tianwei): Prepare resource and add split strategy
    // TODO: set taskType with merge strategy name
    auto status = CommitTaskLogToVersion(taskContext, mergePlan->GetTargetVersion());
    if (!status.IsOK()) {
        return std::pair(status, nullptr);
    }
    status = CommitMergePlans(mergePlan, taskContext);
    if (!status.IsOK()) {
        return std::make_pair(status, nullptr);
    }

    auto indexTaskPlan = std::make_unique<framework::IndexTaskPlan>(_taskName, TASK_TYPE);
    NormalTableMergeDescriptionCreator decriptionCreator(tabletSchema, mergeStrategy->GetName(), compactionType,
                                                         optimize);
    assert(tabletSchema);
    auto [status1, operationDescriptions] = decriptionCreator.CreateMergeOperationDescriptions(mergePlan);
    if (!status1.IsOK() && !status1.IsNotFound()) {
        return std::make_pair(status1, nullptr);
    }
    for (auto& operationDesc : operationDescriptions) {
        indexTaskPlan->AddOperation(operationDesc);
    }
    framework::IndexOperationDescription endTaskOpDesc(/*id*/ operationDescriptions.size(),
                                                       /*type*/ framework::END_TASK_OPERATION);
    const auto& version = mergePlan->GetTargetVersion();
    std::string versionContent;
    status = indexlib::file_system::JsonUtil::ToString(version, &versionContent).Status();
    assert(status.IsOK());
    endTaskOpDesc.AddParameter(PARAM_TARGET_VERSION, versionContent);
    indexTaskPlan->SetEndTaskOperation(endTaskOpDesc);

    return make_pair(Status::OK(), std::move(indexTaskPlan));
}

Status NormalTableCompactPlanCreator::CommitMergePlans(const std::shared_ptr<MergePlan>& mergePlan,
                                                       const framework::IndexTaskContext* taskContext)
{
    auto resourceManager = taskContext->GetResourceManager();
    std::shared_ptr<MergePlan> mergePlanResource;
    auto status = resourceManager->LoadResource(/*name*/ MERGE_PLAN, /*type*/ MERGE_PLAN, mergePlanResource);
    if (status.IsNotFound()) {
        status = resourceManager->CreateResource(/*name*/ MERGE_PLAN, /*type*/ MERGE_PLAN, mergePlanResource);
    }
    RETURN_IF_STATUS_ERROR(status, "load or create resource failed");
    for (size_t i = 0; i < mergePlan->Size(); i++) {
        mergePlanResource->AddMergePlan(mergePlan->GetSegmentMergePlan(i));
    }

    const auto& targetVersion = mergePlan->GetTargetVersion();
    mergePlanResource->SetTargetVersion(targetVersion.Clone());
    return resourceManager->CommitResource(MERGE_PLAN);
}

std::pair<std::string, std::unique_ptr<MergeStrategy>>
NormalTableCompactPlanCreator::CreateCompactStrategy(const std::string& mergeStrategyName, bool isOptimizeMerge)
{
    if (mergeStrategyName == MergeStrategyDefine::OPTIMIZE_MERGE_STRATEGY_NAME or isOptimizeMerge) {
        return {NORMAL_TABLE_MERGE_TYPE, std::make_unique<NormalTabletOptimizeMergeStrategy>()};
    }
    if (mergeStrategyName == MergeStrategyDefine::COMBINED_MERGE_STRATEGY_NAME) {
        auto strategy = std::make_unique<PriorityQueueMergeStrategy>();
        return {NORMAL_TABLE_MERGE_TYPE, std::make_unique<CombinedMergeStrategy>(std::move(strategy))};
    }
    if (mergeStrategyName == MergeStrategyDefine::REALTIME_MERGE_STRATEGY_NAME) {
        return {NORMAL_TABLE_MERGE_TYPE, std::make_unique<RealtimeMergeStrategy>()};
    }
    if (mergeStrategyName == MergeStrategyDefine::PRIORITY_QUEUE_MERGE_STRATEGY_NAME) {
        auto strategy = std::make_unique<PriorityQueueMergeStrategy>();
        return {NORMAL_TABLE_MERGE_TYPE, std::make_unique<CombinedMergeStrategy>(std::move(strategy))};
    }
    if (mergeStrategyName == MergeStrategyDefine::BALANCE_TREE_MERGE_STRATEGY_NAME) {
        auto strategy = std::make_unique<BalanceTreeMergeStrategy>();
        return {NORMAL_TABLE_MERGE_TYPE, std::make_unique<CombinedMergeStrategy>(std::move(strategy))};
    }
    if (mergeStrategyName == MergeStrategyDefine::SPECIFIC_SEGMENTS_MERGE_STRATEGY_NAME) {
        return {NORMAL_TABLE_MERGE_TYPE, std::make_unique<SpecificSegmentsMergeStrategy>()};
    }
    if (mergeStrategyName == MergeStrategyDefine::ADAPTIVE_MERGE_STRATEGY_NAME) {
        return {NORMAL_TABLE_MERGE_TYPE, std::make_unique<AdaptiveMergeStrategy>()};
    }
    AUTIL_LOG(ERROR, "Unsupport merge strategy name [%s]", mergeStrategyName.c_str());
    return {NORMAL_TABLE_MERGE_TYPE, nullptr};
}

std::string NormalTableCompactPlanCreator::ConstructLogTaskType() const
{
    std::string logTypeSuffix = _taskName.empty() ? "_default_" : _taskName;
    return TASK_TYPE + "@" + logTypeSuffix;
}

std::string NormalTableCompactPlanCreator::ConstructLogTaskId(const framework::IndexTaskContext* taskContext,
                                                              const framework::Version& targetVersion) const
{
    const auto& baseVersion = taskContext->GetTabletData()->GetOnDiskVersion();
    std::string taskId = "version_";
    taskId += autil::StringUtil::toString(baseVersion.GetVersionId());
    taskId += "_to_";
    taskId += autil::StringUtil::toString(targetVersion.GetVersionId());
    return taskId;
}

} // namespace indexlibv2::table
