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
#include "indexlib/table/normal_table/index_task/PackAttributeIndexMergeOperation.h"

#include "indexlib/index/IIndexMerger.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"

namespace indexlibv2 { namespace table {
AUTIL_LOG_SETUP(indexlib.table, PackAttributeIndexMergeOperation);

PackAttributeIndexMergeOperation::PackAttributeIndexMergeOperation(const framework::IndexOperationDescription& opDesc)
    : IndexMergeOperation(opDesc)
{
}

Status PackAttributeIndexMergeOperation::Execute(const framework::IndexTaskContext& context)
{
    // TODO(ZQ): support patch & slice
    try {
        return IndexMergeOperation::Execute(context);
    } catch (const autil::legacy::ExceptionBase& e) {
        auto status = Status::InternalError("pack attribute merge failed, exception [%s]", e.what());
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    }
}

}} // namespace indexlibv2::table
