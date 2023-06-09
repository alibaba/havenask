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
#include "indexlib/table/index_task/add_index/PatchSegmentMoveOperation.h"

#include "indexlib/base/PathUtil.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/LogicalFileSystem.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, PatchSegmentMoveOperation);

PatchSegmentMoveOperation::PatchSegmentMoveOperation(const framework::IndexOperationDescription& desc)
    : framework::IndexOperation(desc.GetId(), desc.UseOpFenceDir())
    , _desc(desc)
{
}

PatchSegmentMoveOperation::~PatchSegmentMoveOperation() {}

framework::IndexOperationDescription PatchSegmentMoveOperation::CreateOperationDescription(
    framework::IndexOperationId id, const std::string& targetSegmentName, schemaid_t targetSchemaId,
    const std::vector<framework::IndexOperationId>& dependAddIndexOperaions)
{
    framework::IndexOperationDescription opDesc(id, PatchSegmentMoveOperation::OPERATION_TYPE);

    opDesc.AddParameter(PatchSegmentMoveOperation::PARAM_TARGET_SEGMENT_DIR, targetSegmentName);
    opDesc.AddParameter(PatchSegmentMoveOperation::PARAM_PATCH_SCHEMA_ID, targetSchemaId);
    opDesc.AddParameter(PatchSegmentMoveOperation::PARAM_INDEX_OPERATIONS,
                        autil::legacy::ToJsonString(dependAddIndexOperaions));
    return opDesc;
}

Status PatchSegmentMoveOperation::Execute(const framework::IndexTaskContext& context)
{
    schemaid_t targetSchemaId;
    if (!_desc.GetParameter(PARAM_PATCH_SCHEMA_ID, targetSchemaId)) {
        AUTIL_LOG(ERROR, "get target schema id failed");
        return Status::Corruption();
    }

    std::string segDirName;
    if (!_desc.GetParameter(PARAM_TARGET_SEGMENT_DIR, segDirName)) {
        AUTIL_LOG(ERROR, "get target segment dir from desc failed");
        return Status::Corruption("get target segment dir from desc failed");
    }

    std::vector<framework::IndexOperationId> indexOperations;
    std::string indexOperationsStr;
    if (!_desc.GetParameter(PARAM_INDEX_OPERATIONS, indexOperationsStr)) {
        AUTIL_LOG(ERROR, "get index operations from desc failed");
        return Status::Corruption("get index operations from desc failed");
    }
    autil::legacy::FromJsonString(indexOperations, indexOperationsStr);
    auto rootDirectory = context.GetIndexRoot()->GetIDirectory();
    std::string patchDir = PathUtil::GetPatchIndexDirName(targetSchemaId);
    std::string patchSegmentDir = PathUtil::JoinPath(patchDir, segDirName);
    auto [status, isExist] = rootDirectory->IsExist(patchSegmentDir).StatusWith();
    RETURN_IF_STATUS_ERROR(status, "check exist for segment dir failed");
    if (!isExist) {
        auto [status1, tmpDir] =
            rootDirectory->MakeDirectory(patchSegmentDir, indexlib::file_system::DirectoryOption()).StatusWith();
        RETURN_IF_STATUS_ERROR(status1, "make patch segment dir failed");
    }

    for (const auto& opId : indexOperations) {
        auto status = MoveIndexDir(opId, targetSchemaId, segDirName, context);
        if (!status.IsOK()) {
            return status;
        }
    }
    return Status::OK();
}

Status PatchSegmentMoveOperation::MoveIndexDir(framework::IndexOperationId opId, schemaid_t targetSchemaId,
                                               const std::string& segmentName,
                                               const framework::IndexTaskContext& context)
{
    auto rootDirectory = context.GetIndexRoot()->GetIDirectory();
    std::string patchDir = PathUtil::GetPatchIndexDirName(targetSchemaId);
    std::string patchSegmentDir = PathUtil::JoinPath(patchDir, segmentName);
    auto fenceDirectory = context.GetDependOperationFenceRoot(opId);
    if (!fenceDirectory) {
        AUTIL_LOG(ERROR, "get fence dir of op [%ld] failed", opId);
        return Status::IOError("get fence dir failed");
    }

    std::string opSegmentDir;
    if (!_desc.UseOpFenceDir()) {
        // legacy code, todo delete
        opSegmentDir = PathUtil::JoinPath(framework::IndexOperationDescription::GetOpFenceDirName(opId), segmentName);
    } else {
        opSegmentDir = segmentName;
    }
    std::string physicalPath = fenceDirectory->GetPhysicalPath(opSegmentDir);
    indexlib::file_system::FenceContextPtr fenceContext = rootDirectory->GetFenceContext();
    auto status =
        toStatus(rootDirectory->GetFileSystem()->MergeDirs({physicalPath}, patchSegmentDir, false, fenceContext.get()));
    if (status.IsOK() || status.IsExist()) {
        return Status::OK();
    }
    RETURN_IF_STATUS_ERROR(status, "merge index path [%s] of op[%ld] failed", physicalPath.c_str(), opId);
    return status;
}

} // namespace indexlibv2::table
