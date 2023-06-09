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
#include "indexlib/table/index_task/merger/CommonTaskOperationCreator.h"

#include "indexlib/framework/index_task/IndexOperation.h"
#include "indexlib/table/index_task/merger/EndMergeTaskOperation.h"
#include "indexlib/table/index_task/merger/FenceDirDeleteOperation.h"
#include "indexlib/table/index_task/merger/MergedSegmentMoveOperation.h"
#include "indexlib/table/index_task/merger/MergedVersionCommitOperation.h"
#include "indexlib/table/index_task/merger/PathMoveOperation.h"

using namespace std;

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, CommonTaskOperationCreator);

std::unique_ptr<framework::IndexOperation>
CommonTaskOperationCreator::CreateOperation(const framework::IndexOperationDescription& opDesc)
{
    const auto& typeStr = opDesc.GetType();
    if (typeStr == MergedSegmentMoveOperation::OPERATION_TYPE) {
        return std::make_unique<MergedSegmentMoveOperation>(opDesc);
    }
    if (typeStr == MergedVersionCommitOperation::OPERATION_TYPE) {
        return std::make_unique<MergedVersionCommitOperation>(opDesc);
    }
    if (typeStr == FenceDirDeleteOperation::OPERATION_TYPE) {
        return std::make_unique<FenceDirDeleteOperation>(opDesc);
    }
    if (typeStr == framework::END_TASK_OPERATION) {
        return std::make_unique<EndMergeTaskOperation>(opDesc);
    }
    if (typeStr == PathMoveOperation::OPERATION_TYPE) {
        return std::make_unique<PathMoveOperation>(opDesc);
    }
    return nullptr;
}

} // namespace indexlibv2::table
