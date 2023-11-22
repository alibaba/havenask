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
#include "indexlib/table/kv_table/index_task/KVTableBulkloadPlanCreator.h"

#include "indexlib/config/TabletSchema.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/framework/index_task/IndexTaskContext.h"
#include "indexlib/framework/index_task/IndexTaskPlan.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/table/index_task/merger/EndMergeTaskOperation.h"
#include "indexlib/table/index_task/merger/FenceDirDeleteOperation.h"
#include "indexlib/table/index_task/merger/MergedVersionCommitOperation.h"
#include "indexlib/table/kv_table/index_task/KVTableBulkloadOperation.h"
#include "indexlib/table/kv_table/index_task/KVTableBulkloadSegmentCreateOperation.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, KVTableBulkloadPlanCreator);

const std::string KVTableBulkloadPlanCreator::TASK_TYPE = framework::BULKLOAD_TASK_TYPE;

KVTableBulkloadPlanCreator::KVTableBulkloadPlanCreator(const std::string& taskName, const std::string& taskTraceId,
                                                       const std::map<std::string, std::string>& params)
    : SimpleIndexTaskPlanCreator(taskName, taskTraceId, params)
{
}

std::pair<Status, std::unique_ptr<framework::IndexTaskPlan>>
KVTableBulkloadPlanCreator::CreateTaskPlan(const framework::IndexTaskContext* taskContext)
{
    framework::IndexOperationId id = 0;
    versionid_t targetVersionId = taskContext->GetMaxMergedVersionId() + 1;
    auto [status, bulkloadOpDesc] =
        KVTableBulkloadOperation::CreateOperationDescription(id++, _params, _taskTraceId, targetVersionId);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "create KVTableBulkloadOperation failed, tableName[%s]", taskContext->GetTableName().c_str());
        return {status, nullptr};
    }
    auto bulkloadCreateOpDesc = KVTableBulkloadSegmentCreateOperation::CreateOperationDescription(id++);
    auto commitOpDesc = MergedVersionCommitOperation::CreateOperationDescription(id++, targetVersionId);
    auto delFenceOpDesc = FenceDirDeleteOperation::CreateOperationDescription(id++);

    auto indexTaskPlan = std::make_unique<framework::IndexTaskPlan>(_taskName, TASK_TYPE);
    indexTaskPlan->AddOperation(bulkloadOpDesc);

    bulkloadCreateOpDesc.AddDepend(bulkloadOpDesc.GetId());
    indexTaskPlan->AddOperation(bulkloadCreateOpDesc);

    commitOpDesc.AddDepend(bulkloadCreateOpDesc.GetId());
    indexTaskPlan->AddOperation(commitOpDesc);

    delFenceOpDesc.AddDepend(commitOpDesc.GetId());
    indexTaskPlan->AddOperation(delFenceOpDesc);

    indexTaskPlan->SetEndTaskOperation(EndMergeTaskOperation::CreateOperationDescription(id++, targetVersionId));

    AUTIL_LOG(INFO, "bulkload kv table [%s], taskName[%s], targetVersion[%d]", taskContext->GetTableName().c_str(),
              _taskName.c_str(), targetVersionId);
    return std::make_pair(Status::OK(), std::move(indexTaskPlan));
}

std::string KVTableBulkloadPlanCreator::ConstructLogTaskType() const
{
    std::string logType = TASK_TYPE;
    if (!_taskName.empty()) {
        logType += "@";
        logType += _taskName;
    }
    return logType;
}

} // namespace indexlibv2::table
