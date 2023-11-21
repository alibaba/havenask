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
#include "indexlib/table/kv_table/index_task/KVTableMergePlanCreator.h"

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
#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/table/index_task/merger/MergeStrategy.h"
#include "indexlib/table/index_task/merger/MergeStrategyDefine.h"
#include "indexlib/table/index_task/merger/ShardBasedMergeStrategy.h"
#include "indexlib/table/kv_table/index_task/KVTableMergeDescriptionCreator.h"
#include "indexlib/table/kv_table/index_task/KeyValueOptimizeMergeStrategy.h"

using namespace std;

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, KVTableMergePlanCreator);

const std::string KVTableMergePlanCreator::TASK_TYPE = MERGE_TASK_TYPE;

KVTableMergePlanCreator::KVTableMergePlanCreator(const std::string& taskName, const std::string& taskTraceId,
                                                 const std::map<std::string, std::string>& params)
    : SimpleIndexTaskPlanCreator(taskName, taskTraceId, params)
{
}

KVTableMergePlanCreator::~KVTableMergePlanCreator() {}

std::pair<Status, std::unique_ptr<framework::IndexTaskPlan>>
KVTableMergePlanCreator::CreateTaskPlan(const framework::IndexTaskContext* taskContext)
{
    bool optimize = false;
    if (!taskContext->GetParameter(IS_OPTIMIZE_MERGE, optimize)) {
        optimize = false;
    }

    auto mergeStrategy = CreateMergeStrategy(taskContext);
    if (!mergeStrategy) {
        AUTIL_LOG(ERROR, "create merge strategy failed");
        return std::make_pair(Status::ConfigError(), nullptr);
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

    auto status = CommitTaskLogToVersion(taskContext, mergePlan->GetTargetVersion());
    if (!status.IsOK()) {
        return std::pair(status, nullptr);
    }
    status = CommitMergePlans(mergePlan, taskContext);
    if (!status.IsOK()) {
        return std::make_pair(status, nullptr);
    }

    auto indexTaskPlan = std::make_unique<framework::IndexTaskPlan>(_taskName, TASK_TYPE);
    KVTableMergeDescriptionCreator decriptionCreator(taskContext->GetTabletSchema());
    auto [status1, operationDescriptions] = decriptionCreator.CreateMergeOperationDescriptions(mergePlan);
    if (!status1.IsOK()) {
        AUTIL_LOG(ERROR, "create merge operation desc failed");
        return std::make_pair(status1, nullptr);
    }

    for (auto& operationDesc : operationDescriptions) {
        indexTaskPlan->AddOperation(operationDesc);
    }

    framework::IndexOperationDescription endTaskOpDesc(/*id*/ operationDescriptions.size(),
                                                       /*type*/ framework::END_TASK_OPERATION);
    mergePlan->AddTaskLogInTargetVersion("merge@_default_", taskContext);
    const auto& version = mergePlan->GetTargetVersion();

    std::string versionContent;
    status = indexlib::file_system::JsonUtil::ToString(version, &versionContent).Status();
    assert(status.IsOK());
    endTaskOpDesc.AddParameter(PARAM_TARGET_VERSION, versionContent);
    indexTaskPlan->SetEndTaskOperation(endTaskOpDesc);
    return make_pair(Status::OK(), std::move(indexTaskPlan));
}

Status KVTableMergePlanCreator::CommitMergePlans(const std::shared_ptr<MergePlan>& mergePlan,
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

std::unique_ptr<MergeStrategy>
KVTableMergePlanCreator::CreateMergeStrategy(const framework::IndexTaskContext* taskContext)
{
    auto mergeConfig = taskContext->GetMergeConfig();
    bool optimize = false;
    if (!taskContext->GetParameter(IS_OPTIMIZE_MERGE, optimize)) {
        optimize = false;
    }
    if (optimize || mergeConfig.GetMergeStrategyStr() == MergeStrategyDefine::OPTIMIZE_MERGE_STRATEGY_NAME) {
        return std::make_unique<KeyValueOptimizeMergeStrategy>();
    }
    if (mergeConfig.GetMergeStrategyStr() == MergeStrategyDefine::SHARD_BASED_MERGE_STRATEGY_NAME) {
        return std::make_unique<ShardBasedMergeStrategy>();
    }
    AUTIL_LOG(ERROR, "not support merge strategy name [%s]", mergeConfig.GetMergeStrategyStr().c_str());
    return nullptr;
}

std::string KVTableMergePlanCreator::ConstructLogTaskType() const
{
    std::string logTypeSuffix = _taskName.empty() ? "_default_" : _taskName;
    return TASK_TYPE + "@" + logTypeSuffix;
}

std::string KVTableMergePlanCreator::ConstructLogTaskId(const framework::IndexTaskContext* taskContext,
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
