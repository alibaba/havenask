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
#include "indexlib/table/index_task/merger/PathMoveOperation.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/MergeDirsOption.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, PathMoveOperation);

Status PathMoveOperation::Execute(const framework::IndexTaskContext& context)
{
    std::string movePathStr;
    if (!_desc.GetParameter(PARAM_OP_TO_MOVE_PATHS, movePathStr)) {
        RETURN_STATUS_ERROR(Corruption, "get movePaths failed");
    }
    std::map<std::string, framework::IndexOperationId> movePaths;

    try {
        autil::legacy::FromJsonString(movePaths, movePathStr);
    } catch (const autil::legacy::ExceptionBase& e) {
        RETURN_STATUS_ERROR(Corruption, "from json op_to_move_src_paths param [%s] failed", movePathStr.c_str());
    }

    auto rootDirectory = context.GetIndexRoot()->GetIDirectory();
    for (const auto& [path, opId] : movePaths) {
        auto fenceRootDirectory = context.GetDependOperationFenceRoot(opId);
        if (!fenceRootDirectory) {
            RETURN_STATUS_ERROR(Corruption, "get op [%d] fence root dir failed", opId);
        }
        auto [existStatus, exist] = fenceRootDirectory->IsExist(path).StatusWith();
        RETURN_IF_STATUS_ERROR(existStatus, "is exist for [%s] failed", path.c_str());
        if (!exist) {
            AUTIL_LOG(INFO, "op [%ld] source path [%s] is not exist, move nothing", opId, path.c_str());
            continue;
        }

        std::string physicalPath = fenceRootDirectory->GetPhysicalPath(path);
        auto status = rootDirectory->GetFileSystem()
                          ->MergeDirs({physicalPath}, path, indexlib::file_system::MergeDirsOption::NoMergePackage())
                          .Status();
        RETURN_IF_STATUS_ERROR(status, "move path [%s] to [%s] failed", physicalPath.c_str(),
                               rootDirectory->DebugString().c_str());

        AUTIL_LOG(INFO, "success move path [%s] to [%s]", physicalPath.c_str(), rootDirectory->DebugString().c_str());
    }
    return Status::OK();
}

framework::IndexOperationDescription
PathMoveOperation::CreateOperationDescription(framework::IndexOperationId opId,
                                              const std::map<std::string, framework::IndexOperationId>& movePaths)
{
    framework::IndexOperationDescription opDesc(opId, OPERATION_TYPE);
    opDesc.AddParameter(PARAM_OP_TO_MOVE_PATHS, autil::legacy::ToJsonString(movePaths));
    return opDesc;
}

} // namespace indexlibv2::table
