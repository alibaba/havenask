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
#include "indexlib/table/index_task/merger/FenceDirDeleteOperation.h"

#include "indexlib/file_system/LogicalFileSystem.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"

using namespace std;

namespace indexlibv2 { namespace table {
AUTIL_LOG_SETUP(indexlib.table, FenceDirDeleteOperation);

FenceDirDeleteOperation::FenceDirDeleteOperation(const framework::IndexOperationDescription& opDesc)
    : framework::IndexOperation(opDesc.GetId(), opDesc.UseOpFenceDir())
{
}

FenceDirDeleteOperation::~FenceDirDeleteOperation() {}

framework::IndexOperationDescription FenceDirDeleteOperation::CreateOperationDescription(framework::IndexOperationId id)
{
    return framework::IndexOperationDescription(id, OPERATION_TYPE);
}

Status FenceDirDeleteOperation::Execute(const framework::IndexTaskContext& context)
{
    std::vector<std::string> allFenceDirs;
    auto status = context.GetAllFenceRootInTask(&allFenceDirs);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "get all fence dirs failed");
        return status;
    }

    for (const std::string& dir : allFenceDirs) {
        auto status = indexlib::file_system::toStatus(
            indexlib::file_system::FslibWrapper::DeleteDir(dir, indexlib::file_system::DeleteOption::NoFence(true))
                .Code());
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "delete dir [%s] failed", dir.c_str());
            return status;
        }
    }
    return Status::OK();
}

}} // namespace indexlibv2::table
