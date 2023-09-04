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
#include "indexlib/table/normal_table/NormalDiskSegment.h"

#include "indexlib/config/IIndexConfig.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/index/IDiskIndexer.h"
#include "indexlib/index/IIndexFactory.h"
#include "indexlib/index/IndexFactoryCreator.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"
#include "indexlib/index/primary_key/Common.h"
#include "indexlib/table/normal_table/Common.h"

namespace indexlibv2::table {
namespace {
using indexlib::index::OPERATION_LOG_INDEX_NAME;
using indexlib::index::OPERATION_LOG_INDEX_TYPE_STR;
} // namespace
AUTIL_LOG_SETUP(indexlib.table, NormalDiskSegment);

NormalDiskSegment::NormalDiskSegment(const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema,
                                     const framework::SegmentMeta& segmentMeta,
                                     const framework::BuildResource& buildResource)
    : PlainDiskSegment(schema, segmentMeta, buildResource)
{
}

NormalDiskSegment::~NormalDiskSegment() {}

size_t NormalDiskSegment::EstimateMemUsed(const std::shared_ptr<config::ITabletSchema>& schema)
{
    auto segDir = GetSegmentDirectory();
    assert(segDir);
    size_t totalMemUsed = 0;
    auto indexerParam = GenerateIndexerParameter();
    auto indexFactoryCreator = index::IndexFactoryCreator::GetInstance();
    auto indexConfigs = schema->GetIndexConfigs();
    for (const auto& indexConfig : indexConfigs) {
        std::string indexType = indexConfig->GetIndexType();
        std::string indexName = indexConfig->GetIndexName();
        if ((indexType == VIRTUAL_ATTRIBUTE_INDEX_TYPE_STR || indexType == OPERATION_LOG_INDEX_TYPE_STR) &&
            IsMergedSegmentId(GetSegmentId())) {
            continue;
        }
        if (_segmentMeta.schema && _segmentMeta.schema->GetIndexConfig(indexType, indexName) == nullptr) {
            continue;
        }
        auto [status, indexFactory] = indexFactoryCreator->Create(indexType);
        if (!status.IsOK()) {
            continue;
        }
        auto indexer = indexFactory->CreateDiskIndexer(indexConfig, indexerParam);
        if (!indexer) {
            continue;
        }

        auto [dirStatus, indexDir] = GetIndexDirectory(segDir, indexConfig, indexFactory);
        if (!dirStatus.IsOK()) {
            AUTIL_LOG(ERROR, "get index directory fail, error [%s]", dirStatus.ToString().c_str());
            continue;
        }
        try {
            totalMemUsed += indexer->EstimateMemUsed(indexConfig, (indexDir ? indexDir->GetIDirectory() : nullptr));
        } catch (...) {
            AUTIL_LOG(ERROR, "fail to estimate mem use for indexer [%s]", indexName.c_str());
            continue;
        }
    }
    return totalMemUsed;
}

bool NormalDiskSegment::NeedDrop(const std::string& indexType, const std::string& indexName,
                                 const std::vector<std::shared_ptr<config::ITabletSchema>>& schemas)
{
    if (indexType == OPERATION_LOG_INDEX_TYPE_STR) {
        return true;
    }
    return PlainDiskSegment::NeedDrop(indexType, indexName, schemas);
}

std::pair<Status, std::vector<plain::DiskIndexerItem>>
NormalDiskSegment::OpenIndexer(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig)
{
    std::vector<plain::DiskIndexerItem> results;
    if (!indexConfig) {
        AUTIL_LOG(ERROR, "index config is nullptr");
        return std::make_pair(Status::InvalidArgs("config is nullptr"), results);
    }
    auto indexType = indexConfig->GetIndexType();
    if (indexType == OPERATION_LOG_INDEX_TYPE_STR && IsMergedSegmentId(GetSegmentId())) {
        return std::make_pair(Status::OK(), results);
    }
    if (indexType == VIRTUAL_ATTRIBUTE_INDEX_TYPE_STR && !IsPrivateSegmentId(GetSegmentId())) {
        return std::make_pair(Status::OK(), results);
    }

    std::pair<Status, std::vector<plain::DiskIndexerItem>> ret;
    if (indexType == index::PRIMARY_KEY_INDEX_TYPE_STR) {
        // optimize to save mem. primary key indexer will not be opened during plain built segment open.
        // in normal table, primary key is opened by its own primary bkey reader.
        auto indexFactoryCreator = index::IndexFactoryCreator::GetInstance();
        auto [factoryStatus, indexFactory] = indexFactoryCreator->Create(index::PRIMARY_KEY_INDEX_TYPE_STR);
        if (!factoryStatus.IsOK()) {
            AUTIL_LOG(ERROR, "create primarykey index factory for index [%s] failed",
                      indexConfig->GetIndexName().c_str());
            return std::make_pair(factoryStatus, std::vector<plain::DiskIndexerItem> {});
        }
        auto [status, pkIndexer] = CreateSingleIndexer(indexFactory, indexConfig);
        ret.first = status;
        ret.second.push_back({indexType, indexConfig->GetIndexName(), pkIndexer});
    } else {
        ret = PlainDiskSegment::OpenIndexer(indexConfig);
    }
    auto& [status, indexerItemVec] = ret;
    RETURN2_IF_STATUS_ERROR(status, results, "open indexer failed.");

    results = std::move(indexerItemVec);
    auto invertedIndexConfig = std::dynamic_pointer_cast<config::InvertedIndexConfig>(indexConfig);
    if (invertedIndexConfig == nullptr) {
        return {Status::OK(), results};
    }
    auto appendTruncateIndexer = [&results,
                                  this](const std::shared_ptr<config::InvertedIndexConfig>& invertedConfig) -> Status {
        for (const auto& truncateIndexConfig : invertedConfig->GetTruncateIndexConfigs()) {
            auto [st, items] = PlainDiskSegment::OpenIndexer(truncateIndexConfig);
            RETURN_IF_STATUS_ERROR(st, "open truncate indexer failed.");
            results.insert(results.end(), items.begin(), items.end());
        }
        return Status::OK();
    };
    if (invertedIndexConfig->GetShardingType() == config::InvertedIndexConfig::IST_NEED_SHARDING) {
        for (const auto& shardConfig : invertedIndexConfig->GetShardingIndexConfigs()) {
            auto shardInvertedIndexConfig = std::dynamic_pointer_cast<config::InvertedIndexConfig>(shardConfig);
            assert(shardInvertedIndexConfig != nullptr);
            RETURN2_IF_STATUS_ERROR(appendTruncateIndexer(shardInvertedIndexConfig), results,
                                    "open truncate indexer for [%s] failed",
                                    shardInvertedIndexConfig->GetIndexName().c_str());
        }
    } else {
        RETURN2_IF_STATUS_ERROR(appendTruncateIndexer(invertedIndexConfig), results,
                                "open truncate indexer for [%s] failed", invertedIndexConfig->GetIndexName().c_str());
    }
    return {Status::OK(), results};
}

std::pair<Status, std::shared_ptr<indexlib::file_system::Directory>>
NormalDiskSegment::GetIndexDirectory(const std::shared_ptr<indexlib::file_system::Directory>& segmentDir,
                                     const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                                     index::IIndexFactory* indexFactory)
{
    auto indexPath = indexFactory->GetIndexPath();
    auto indexDir = segmentDir->GetDirectory(indexPath, /*throwExceptionIfNotExist=*/false);
    if (indexDir == nullptr) {
        AUTIL_LOG(DEBUG, "get built index dir [%s] failed", indexPath.c_str());
    }
    return std::make_pair(Status::OK(), indexDir);
}

} // namespace indexlibv2::table
