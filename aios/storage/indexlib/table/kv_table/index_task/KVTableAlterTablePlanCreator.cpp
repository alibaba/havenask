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
#include "indexlib/table/kv_table/index_task/KVTableAlterTablePlanCreator.h"

#include "indexlib/config/TabletSchema.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/framework/index_task/IndexTaskContext.h"
#include "indexlib/framework/index_task/IndexTaskPlan.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/table/index_task/merger/EndMergeTaskOperation.h"
#include "indexlib/table/index_task/merger/FenceDirDeleteOperation.h"
#include "indexlib/table/index_task/merger/MergedVersionCommitOperation.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, KVTableAlterTablePlanCreator);

const std::string KVTableAlterTablePlanCreator::TASK_TYPE = framework::ALTER_TABLE_TASK_TYPE;

KVTableAlterTablePlanCreator::KVTableAlterTablePlanCreator(const std::string& taskName, const std::string& taskTraceId,
                                                           const std::map<std::string, std::string>& params)
    : SimpleIndexTaskPlanCreator(taskName, taskTraceId, params)
{
}

KVTableAlterTablePlanCreator::~KVTableAlterTablePlanCreator() {}

std::pair<Status, std::unique_ptr<framework::IndexTaskPlan>>
KVTableAlterTablePlanCreator::CreateTaskPlan(const framework::IndexTaskContext* taskContext)
{
    const auto& version = taskContext->GetTabletData()->GetOnDiskVersion();
    auto baseSchemaId = version.GetReadSchemaId();
    auto targetSchemaId = version.GetSchemaId();
    auto baseSchema = taskContext->GetTabletSchema(baseSchemaId);
    if (!baseSchema) {
        AUTIL_LOG(ERROR, "load base schema [%d] from index root failed", baseSchemaId);
        return std::make_pair(Status::Corruption(), nullptr);
    }
    auto targetSchema = taskContext->GetTabletSchema(targetSchemaId);
    if (!targetSchema) {
        AUTIL_LOG(ERROR, "load target schema [%d] from index root failed", targetSchemaId);
        return std::make_pair(Status::Corruption(), nullptr);
    }

    auto indexTaskPlan = std::make_unique<framework::IndexTaskPlan>(_taskName, TASK_TYPE);
    // prepare version commit operation
    auto targetVersion = version;
    targetVersion.SetVersionId(taskContext->GetMaxMergedVersionId() + 1);
    targetVersion.SetSchemaId(targetSchemaId);
    targetVersion.SetFenceName("");
    targetVersion.SetReadSchemaId(targetSchemaId);

    auto status = CommitTaskLogToVersion(taskContext, targetVersion);
    RETURN2_IF_STATUS_ERROR(status, nullptr, "commit task log to version failed.");

    AUTIL_LOG(INFO, "Alter kv table [%s] with default value strategy, schemaId[%d], versionId [%d]",
              targetSchema->GetTableName().c_str(), targetSchemaId, targetVersion.GetVersionId());
    framework::IndexOperationId id = 0;
    auto commitOpDesc =
        MergedVersionCommitOperation::CreateOperationDescription(id++, targetVersion, /*patchIndexDir*/ "",
                                                                 /*dropIndexes*/ {});
    indexTaskPlan->AddOperation(commitOpDesc);

    auto delFenceOpDesc = FenceDirDeleteOperation::CreateOperationDescription(id);
    delFenceOpDesc.AddDepend(commitOpDesc.GetId());
    indexTaskPlan->AddOperation(delFenceOpDesc);

    id++;
    indexTaskPlan->SetEndTaskOperation(EndMergeTaskOperation::CreateOperationDescription(id, targetVersion));
    return std::make_pair(Status::OK(), std::move(indexTaskPlan));
}

std::string KVTableAlterTablePlanCreator::ConstructLogTaskType() const
{
    std::string logType = TASK_TYPE;
    if (!_taskName.empty()) {
        logType += "@";
        logType += _taskName;
    }
    return logType;
}

std::string KVTableAlterTablePlanCreator::ConstructLogTaskId(const framework::IndexTaskContext* taskContext,
                                                             const framework::Version& targetVersion) const
{
    const auto& baseVersion = taskContext->GetTabletData()->GetOnDiskVersion();
    std::string taskId = "schema_";
    taskId += autil::StringUtil::toString(baseVersion.GetReadSchemaId());
    taskId += "_to_";
    taskId += autil::StringUtil::toString(targetVersion.GetSchemaId());
    return taskId;
}

} // namespace indexlibv2::table
