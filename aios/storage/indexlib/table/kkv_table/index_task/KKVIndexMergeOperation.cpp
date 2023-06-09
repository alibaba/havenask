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
#include "indexlib/table/kkv_table/index_task/KKVIndexMergeOperation.h"

#include "indexlib/config/TabletSchema.h"
#include "indexlib/index/kkv/merge/KKVMerger.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, KKVIndexMergeOperation);

KKVIndexMergeOperation::KKVIndexMergeOperation(const framework::IndexOperationDescription& desc)
    : MultiShardIndexMergeOperation(desc)
{
}

KKVIndexMergeOperation::~KKVIndexMergeOperation() {}

Status KKVIndexMergeOperation::Execute(const framework::IndexTaskContext& context)
{
    // TODO(xinfei.sxf) call InitIgnoreFieldCalculater for kkv merge
    // auto kvMerger = std::dynamic_pointer_cast<index::KVMerger>(_indexMerger);
    // if (kvMerger == nullptr) {
    //     return Status::InternalError("indexMerger is not KVMerger.");
    // }
    // auto st = kvMerger->InitIgnoreFieldCalculater(context.GetTabletData().get());
    // if (!st.IsOK()) {
    //     return st;
    // }
    AUTIL_LOG(INFO, "Execute Merge for kkv tablet [%s] with schemaId [%d]",
              context.GetTabletData()->GetTabletName().c_str(), context.GetTabletSchema()->GetSchemaId());
    return IndexMergeOperation::Execute(context);
}

} // namespace indexlibv2::table
