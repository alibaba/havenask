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
#include "indexlib/table/normal_table/index_task/DropOpLogOperation.h"

#include "indexlib/base/PathUtil.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/operation_log/Common.h"
#include "indexlib/index/operation_log/OperationFieldInfo.h"
#include "indexlib/index/operation_log/OperationLogConfig.h"

namespace indexlibv2::table {
namespace {
using indexlib::index::OPERATION_LOG_INDEX_NAME;
using indexlib::index::OPERATION_LOG_INDEX_TYPE_STR;
using indexlib::index::OperationFieldInfo;
using indexlib::index::OperationLogConfig;
} // namespace
AUTIL_LOG_SETUP(indexlib.table, DropOpLogOperation);

DropOpLogOperation::DropOpLogOperation(const framework::IndexOperationDescription& desc)
    : framework::IndexOperation(desc.GetId(), desc.UseOpFenceDir())
    , _desc(desc)
{
}

DropOpLogOperation::~DropOpLogOperation() {}

bool DropOpLogOperation::NeedDropOpLog(segmentid_t segmentId, const std::shared_ptr<config::TabletSchema>& baseSchema,
                                       const std::shared_ptr<config::TabletSchema>& targetSchema)
{
    if (framework::Segment::IsMergedSegmentId(segmentId)) {
        return false;
    }
    auto config = baseSchema->GetIndexConfig(OPERATION_LOG_INDEX_TYPE_STR, OPERATION_LOG_INDEX_NAME);
    auto opLogConfig = std::dynamic_pointer_cast<OperationLogConfig>(config);
    if (!opLogConfig) {
        return false;
    }
    auto dropFields = DropOpLogOperation::CalculateDropFields(baseSchema, targetSchema);
    return dropFields.size() > 0;
}

std::set<fieldid_t> DropOpLogOperation::CalculateDropFields(const std::shared_ptr<config::TabletSchema>& baseSchema,
                                                            const std::shared_ptr<config::TabletSchema>& targetSchema)
{
    auto config = baseSchema->GetIndexConfig(OPERATION_LOG_INDEX_TYPE_STR, OPERATION_LOG_INDEX_NAME);
    auto opLogConfig = std::dynamic_pointer_cast<OperationLogConfig>(config);
    std::set<fieldid_t> dropFields;
    if (!opLogConfig) {
        return dropFields;
    }
    auto configs = baseSchema->GetIndexConfigs();
    for (auto indexConfig : configs) {
        if (targetSchema->GetIndexConfig(indexConfig->GetIndexType(), indexConfig->GetIndexName())) {
            continue;
        }
        if (!opLogConfig->HasIndexConfig(indexConfig->GetIndexType(), indexConfig->GetIndexName())) {
            continue;
        }
        auto fieldConfigs = indexConfig->GetFieldConfigs();
        for (auto fieldConfig : fieldConfigs) {
            dropFields.insert(fieldConfig->GetFieldId());
        }
    }
    return dropFields;
}

framework::IndexOperationDescription DropOpLogOperation::CreateOperationDescription(framework::IndexOperationId id,
                                                                                    segmentid_t targetSegmentId,
                                                                                    schemaid_t targetSchema)
{
    framework::IndexOperationDescription opDesc(id, DropOpLogOperation::OPERATION_TYPE);
    opDesc.AddParameter(SEGMENT_ID, targetSegmentId);
    opDesc.AddParameter(TARGET_SCHEMA_ID, targetSchema);
    return opDesc;
}

Status DropOpLogOperation::Execute(const framework::IndexTaskContext& context)
{
    schemaid_t targetSchemaId;
    if (!_desc.GetParameter(TARGET_SCHEMA_ID, targetSchemaId)) {
        AUTIL_LOG(ERROR, "get target schema id failed");
        return Status::Corruption();
    }
    segmentid_t targetSegmentId;
    if (!_desc.GetParameter(SEGMENT_ID, targetSegmentId)) {
        AUTIL_LOG(ERROR, "get target segment id failed");
        return Status::Corruption();
    }
    auto tabletData = context.GetTabletData();
    auto segment = tabletData->GetSegment(targetSegmentId);
    auto baseSchema = segment->GetSegmentSchema();
    auto targetSchema = tabletData->GetTabletSchema(targetSchemaId);
    assert(baseSchema);
    assert(targetSchema);
    auto fieldIds = CalculateDropFields(baseSchema, targetSchema);
    auto segmentDirectory = segment->GetSegmentDirectory()->GetIDirectory();
    auto opLogConfig = baseSchema->GetIndexConfig(OPERATION_LOG_INDEX_TYPE_STR, OPERATION_LOG_INDEX_NAME);
    auto opLogPaths = opLogConfig->GetIndexPath();
    auto opLogPath = opLogPaths[0];
    assert(opLogPaths.size() == 1);
    auto [status, opLogDir] = segmentDirectory->GetDirectory(opLogPath).StatusWith();
    RETURN_IF_STATUS_ERROR(status, "get op log path failed");
    std::shared_ptr<OperationFieldInfo> operationFieldInfo;
    status = OperationFieldInfo::GetLatestOperationFieldInfo(opLogDir, operationFieldInfo);
    RETURN_IF_STATUS_ERROR(status, "get latest drop operation info failed");
    if (!operationFieldInfo) {
        operationFieldInfo.reset(new OperationFieldInfo());
        auto actualConfig = std::dynamic_pointer_cast<OperationLogConfig>(opLogConfig);
        operationFieldInfo->Init(actualConfig);
    } else {
        operationFieldInfo->IncreaseId();
    }
    for (auto fieldId : fieldIds) {
        operationFieldInfo->DropField(fieldId);
    }
    std::shared_ptr<indexlib::file_system::IDirectory> fenceDir;
    std::tie(status, fenceDir) = context.GetOpFenceRoot(_desc.GetId(), _desc.UseOpFenceDir());
    auto opFenceDir = fenceDir;
    RETURN_IF_STATUS_ERROR(status, "get op fence dir failed");
    if (!_desc.UseOpFenceDir()) {
        status =
            fenceDir->RemoveDirectory(_desc.GetOpFenceDirName(), indexlib::file_system::RemoveOption::MayNonExist())
                .Status();
        RETURN_IF_STATUS_ERROR(status, "clear exist op dir failed");
        std::tie(status, opFenceDir) =
            fenceDir->MakeDirectory(_desc.GetOpFenceDirName(), indexlib::file_system::DirectoryOption()).StatusWith();
        RETURN_IF_STATUS_ERROR(status, "make op fence dir failed");
    }
    auto segmentPath = tabletData->GetOnDiskVersion().GetSegmentDirName(targetSegmentId);
    auto targetPath = PathUtil::JoinPath(segmentPath, opLogPath);
    auto [status2, indexDir] =
        opFenceDir->MakeDirectory(targetPath, indexlib::file_system::DirectoryOption()).StatusWith();
    RETURN_IF_STATUS_ERROR(status2, "make index dir [%s] failed", targetPath.c_str());
    return operationFieldInfo->Store(indexDir);
}

} // namespace indexlibv2::table
