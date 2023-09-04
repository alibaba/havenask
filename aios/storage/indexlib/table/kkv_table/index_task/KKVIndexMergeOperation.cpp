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

#include "indexlib/config/TabletOptions.h"
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

    std::string indexType;
    if (!_desc.GetParameter("merge_index_type", indexType)) {
        return Status::Corruption("get index name failed");
    }
    // NOTE raw_key index is kv merger
    if (indexlibv2::index::KKV_INDEX_TYPE_STR == indexType) {
        auto kkvMerger = std::dynamic_pointer_cast<index::KKVMerger>(_indexMerger);
        if (nullptr == kkvMerger) {
            return Status::InternalError("indexMerger is not KKVMerger.");
        }
        auto [status, storeTs] = GetStoreTs(context);
        if (!status.IsOK()) {
            return status;
        }
        kkvMerger->SetStoreTs(storeTs);
    }

    AUTIL_LOG(INFO, "Execute Merge for kkv tablet [%s] with schemaId [%d]",
              context.GetTabletData()->GetTabletName().c_str(), context.GetTabletSchema()->GetSchemaId());
    return IndexMergeOperation::Execute(context);
}

std::pair<Status, bool> KKVIndexMergeOperation::GetStoreTs(const framework::IndexTaskContext& context)
{
    bool storeTs = false;
    bool isOptimizeMerge = false;
    if (!context.GetParameter("optimize_merge", isOptimizeMerge)) {
        isOptimizeMerge = false;
    }
    bool fullIndexStoreKKVTs = false;
    std::string path = "offline_index_config.full_index_store_kkv_ts";
    auto status = context.GetTabletOptions()->GetFromRawJson(path, &fullIndexStoreKKVTs);
    if (!status.IsOK() && !status.IsNotFound()) {
        return {status, storeTs};
    }
    bool useUserTimestamp = false;
    path = "offline_index_config.build_config.use_user_timestamp";
    status = context.GetTabletOptions()->GetFromRawJson(path, &useUserTimestamp);
    if (!status.IsOK() && !status.IsNotFound()) {
        return {status, storeTs};
    }
    storeTs = (!isOptimizeMerge || fullIndexStoreKKVTs || useUserTimestamp);
    return {Status::OK(), storeTs};
}

} // namespace indexlibv2::table
