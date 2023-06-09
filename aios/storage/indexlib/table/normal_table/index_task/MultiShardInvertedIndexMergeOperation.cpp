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
#include "indexlib/table/normal_table/index_task/MultiShardInvertedIndexMergeOperation.h"

#include "indexlib/config/TabletSchema.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, MultiShardInvertedIndexMergeOperation);

MultiShardInvertedIndexMergeOperation::MultiShardInvertedIndexMergeOperation(
    const framework::IndexOperationDescription& opDesc)
    : InvertedIndexMergeOperation(opDesc)
{
}

MultiShardInvertedIndexMergeOperation::~MultiShardInvertedIndexMergeOperation() {}

std::pair<Status, std::shared_ptr<config::IIndexConfig>>
MultiShardInvertedIndexMergeOperation::GetIndexConfig(const framework::IndexOperationDescription& opDesc,
                                                      const std::string& indexType, const std::string& indexName,
                                                      const std::shared_ptr<config::TabletSchema>& schema)
{
    std::string shardingIndexName;
    if (!opDesc.GetParameter(SHARD_INDEX_NAME, shardingIndexName)) {
        AUTIL_LOG(ERROR, "get shard index name failed");
        assert(false);
        return std::make_pair(Status::Corruption("get index type failed"), nullptr);
    }
    const auto& indexConfig = std::dynamic_pointer_cast<indexlibv2::config::InvertedIndexConfig>(
        schema->GetIndexConfig(indexType, shardingIndexName));
    if (!indexConfig) {
        AUTIL_LOG(ERROR, "get index config failed. index type [%s], index name [%s]", indexType.c_str(),
                  shardingIndexName.c_str());
        assert(false);
        return std::make_pair(Status::Corruption("get index config failed"), nullptr);
    }
    for (const auto& shardingIndexConfig : indexConfig->GetShardingIndexConfigs()) {
        if (shardingIndexConfig->GetIndexName() == indexName) {
            return std::make_pair(Status::OK(), shardingIndexConfig);
        }
    }
    auto status =
        Status::Corruption("can not find sharding index config for shard index [%s]", shardingIndexName.c_str());
    AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
    return std::make_pair(status, nullptr);
}

} // namespace indexlibv2::table
