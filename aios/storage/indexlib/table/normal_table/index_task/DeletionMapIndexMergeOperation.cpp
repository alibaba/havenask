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
#include "indexlib/table/normal_table/index_task/DeletionMapIndexMergeOperation.h"

#include "indexlib/file_system/IDirectory.h"
#include "indexlib/index/deletionmap/DeletionMapMerger.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/table/normal_table/index_task/Common.h"
#include "indexlib/table/normal_table/index_task/PatchedDeletionMapLoader.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, DeletionMapIndexMergeOperation);

DeletionMapIndexMergeOperation::DeletionMapIndexMergeOperation(const framework::IndexOperationDescription& opDesc)
    : IndexMergeOperation(opDesc)
{
}

DeletionMapIndexMergeOperation::~DeletionMapIndexMergeOperation() {}

Status DeletionMapIndexMergeOperation::Execute(const framework::IndexTaskContext& context)
{
    AUTIL_LOG(INFO, "DeletionMapIndexMergeOperation execute begin");
    auto deletionMapMerger = std::dynamic_pointer_cast<index::DeletionMapMerger>(_indexMerger);
    if (!deletionMapMerger) {
        AUTIL_LOG(ERROR, "get deletion map merger failed");
        return Status::InvalidArgs("get deletion map merger failed");
    }

    framework::IndexOperationId opLog2PatchOperationId;
    std::shared_ptr<indexlib::file_system::Directory> patchDir;
    if (_desc.GetParameter(DEPENDENT_OPERATION_ID, opLog2PatchOperationId)) {
        std::shared_ptr<indexlib::file_system::IDirectory> opLog2PatchOpRootDir =
            context.GetDependOperationFenceRoot(opLog2PatchOperationId);
        if (opLog2PatchOpRootDir == nullptr) {
            AUTIL_LOG(ERROR, "Get op log2 patch dir failed, op id [%ld]", opLog2PatchOperationId)
            return Status::Corruption("Get op log2 patch dir failed");
        }
        auto fsResult = opLog2PatchOpRootDir->GetDirectory(OPERATION_LOG_TO_PATCH_WORK_DIR);
        if (fsResult.Code() != indexlib::file_system::ErrorCode::FSEC_NOENT) {
            auto [status, opLog2PatchWorkDir] = fsResult.StatusWith();
            RETURN_IF_STATUS_ERROR(status, "Get work dir [%s] of op [%ld] failed", OPERATION_LOG_TO_PATCH_WORK_DIR,
                                   opLog2PatchOperationId);
            auto [archiveStatus, opLog2PatchArchiveDir] = opLog2PatchWorkDir->CreateArchiveDirectory("").StatusWith();
            RETURN_IF_STATUS_ERROR(archiveStatus, "Open archive work dir [%s] of op [%ld] failed",
                                   OPERATION_LOG_TO_PATCH_WORK_DIR, opLog2PatchOperationId);
            auto patchDirResult = opLog2PatchArchiveDir->GetDirectory(index::DELETION_MAP_INDEX_PATH);
            if (!patchDirResult.OK()) {
                AUTIL_LOG(ERROR, "get deletion map patch dir failed");
                return Status::Corruption("get deletion map patch dir failed");
            }
            patchDir = indexlib::file_system::IDirectory::ToLegacyDirectory(patchDirResult.Value());
        }
    }

    std::vector<std::pair<segmentid_t, std::shared_ptr<index::DeletionMapDiskIndexer>>> patchedIndexers;
    RETURN_IF_STATUS_ERROR(PatchedDeletionMapLoader::GetPatchedDeletionMapDiskIndexers(context.GetTabletData(),
                                                                                       patchDir, &patchedIndexers),
                           "get patch deletion map indexer from tablet and patch dir failed");
    deletionMapMerger->CollectAllDeletionMap(patchedIndexers);
    auto st = IndexMergeOperation::Execute(context);
    RETURN_IF_STATUS_ERROR(st, "deletion map index merge operation execute failed");
    AUTIL_LOG(INFO, "DeletionMapIndexMergeOperation execute end");
    return Status::OK();
}

} // namespace indexlibv2::table
