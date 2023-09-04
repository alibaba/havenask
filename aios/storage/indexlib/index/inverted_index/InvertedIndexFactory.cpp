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
#include "indexlib/index/inverted_index/InvertedIndexFactory.h"

#include <typeinfo>

#include "indexlib/config/MergeConfig.h"
#include "indexlib/framework/MetricsManager.h"
#include "indexlib/index/IDiskIndexer.h"
#include "indexlib/index/IIndexMerger.h"
#include "indexlib/index/IIndexReader.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/inverted_index/InvertedDiskIndexer.h"
#include "indexlib/index/inverted_index/InvertedIndexMetrics.h"
#include "indexlib/index/inverted_index/InvertedIndexReaderImpl.h"
#include "indexlib/index/inverted_index/InvertedMemIndexer.h"
#include "indexlib/index/inverted_index/MultiShardInvertedDiskIndexer.h"
#include "indexlib/index/inverted_index/MultiShardInvertedIndexReader.h"
#include "indexlib/index/inverted_index/MultiShardInvertedMemIndexer.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapIndexReader.h"
#include "indexlib/index/inverted_index/builtin_index/date/DateDiskIndexer.h"
#include "indexlib/index/inverted_index/builtin_index/date/DateIndexMerger.h"
#include "indexlib/index/inverted_index/builtin_index/date/DateIndexReader.h"
#include "indexlib/index/inverted_index/builtin_index/date/DateMemIndexer.h"
#include "indexlib/index/inverted_index/builtin_index/expack/ExpackIndexMerger.h"
#include "indexlib/index/inverted_index/builtin_index/number/NumberIndexMerger.h"
#include "indexlib/index/inverted_index/builtin_index/pack/PackIndexMerger.h"
#include "indexlib/index/inverted_index/builtin_index/range/RangeDiskIndexer.h"
#include "indexlib/index/inverted_index/builtin_index/range/RangeIndexMerger.h"
#include "indexlib/index/inverted_index/builtin_index/range/RangeIndexReader.h"
#include "indexlib/index/inverted_index/builtin_index/range/RangeMemIndexer.h"
#include "indexlib/index/inverted_index/builtin_index/spatial/SpatialIndexMerger.h"
#include "indexlib/index/inverted_index/builtin_index/spatial/SpatialIndexReader.h"
#include "indexlib/index/inverted_index/builtin_index/string/StringIndexMerger.h"
#include "indexlib/index/inverted_index/builtin_index/text/TextIndexMerger.h"
#include "indexlib/index/inverted_index/config/DateIndexConfig.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"
#include "indexlib/index/inverted_index/config/PackageIndexConfig.h"
#include "indexlib/index/inverted_index/config/RangeIndexConfig.h"
#include "indexlib/index/inverted_index/config/SingleFieldIndexConfig.h"
#include "indexlib/index/inverted_index/config/SpatialIndexConfig.h"
#include "indexlib/index/inverted_index/config/TruncateStrategy.h"

namespace indexlib::index {
namespace {
using indexlibv2::config::IIndexConfig;
using indexlibv2::config::InvertedIndexConfig;
using indexlibv2::index::IDiskIndexer;
using indexlibv2::index::IIndexMerger;
using indexlibv2::index::IIndexReader;
using indexlibv2::index::IMemIndexer;
using indexlibv2::index::IndexerParameter;
} // namespace

AUTIL_LOG_SETUP(indexlib.index, InvertedIndexFactory);

bool InvertedIndexFactory::CheckSupport(InvertedIndexType indexType) const
{
    switch (indexType) {
    case it_number_int8:
    case it_number_uint8:
    case it_number_int16:
    case it_number_uint16:
    case it_number_int32:
    case it_number_uint32:
    case it_number_int64:
    case it_number_uint64:
    case it_text:
    case it_string:
    case it_pack:
    case it_expack:
    case it_spatial:
    case it_range:
    case it_date:
        return true;
    default:
        break;
    }
    AUTIL_LOG(ERROR, "unsupported inverted index type[%d]", indexType);
    return false;
}

std::shared_ptr<IDiskIndexer> InvertedIndexFactory::CreateDiskIndexer(const std::shared_ptr<IIndexConfig>& indexConfig,
                                                                      const IndexerParameter& indexerParam) const
{
    auto config = std::dynamic_pointer_cast<InvertedIndexConfig>(indexConfig);
    if (!config) {
        AUTIL_LOG(ERROR, "failed to cast IIndexConfig to InvertedIndexConfig, indexName[%s], indexType[%s]",
                  indexConfig->GetIndexName().c_str(), indexConfig->GetIndexType().c_str());
        assert(config);
        return nullptr;
    }
    auto indexType = config->GetInvertedIndexType();
    if (!CheckSupport(indexType)) {
        AUTIL_LOG(ERROR, "create disk indexer failed, indexType[%s] indexName[%s]", indexConfig->GetIndexType().c_str(),
                  indexConfig->GetIndexName().c_str());
        return nullptr;
    }
    InvertedIndexConfig::IndexShardingType shardingType = config->GetShardingType();
    switch (shardingType) {
    case InvertedIndexConfig::IndexShardingType::IST_NO_SHARDING:
    case InvertedIndexConfig::IndexShardingType::IST_IS_SHARDING: {
        switch (indexType) {
        case it_range:
            return std::make_shared<RangeDiskIndexer>(indexerParam);
        case it_date:
            return std::make_shared<DateDiskIndexer>(indexerParam);
        default:
            return std::make_shared<InvertedDiskIndexer>(indexerParam);
        }
    }
    case InvertedIndexConfig::IndexShardingType::IST_NEED_SHARDING:
        return std::make_shared<MultiShardInvertedDiskIndexer>(indexerParam);
    default:
        return nullptr;
    }
}

std::shared_ptr<IMemIndexer> InvertedIndexFactory::CreateMemIndexer(const std::shared_ptr<IIndexConfig>& indexConfig,
                                                                    const IndexerParameter& indexerParam) const
{
    auto config = std::dynamic_pointer_cast<InvertedIndexConfig>(indexConfig);
    if (!config) {
        AUTIL_LOG(ERROR, "failed to cast IIndexConfig to InvertedIndexConfig, indexName[%s], indexType[%s]",
                  indexConfig->GetIndexName().c_str(), indexConfig->GetIndexType().c_str());
        assert(config);
        return nullptr;
    }
    auto indexType = config->GetInvertedIndexType();
    if (!CheckSupport(indexType)) {
        AUTIL_LOG(ERROR, "create mem indexer failed, indexType[%s] indexName[%s]", indexConfig->GetIndexType().c_str(),
                  indexConfig->GetIndexName().c_str());
        return nullptr;
    }
    InvertedIndexConfig::IndexShardingType shardingType = config->GetShardingType();
    auto invertedIndexMetrics = InvertedIndexMetrics::Create(indexConfig, indexerParam);
    switch (shardingType) {
    case InvertedIndexConfig::IndexShardingType::IST_NO_SHARDING:
    case InvertedIndexConfig::IndexShardingType::IST_IS_SHARDING: {
        switch (indexType) {
        case it_range:
            return std::make_shared<RangeMemIndexer>(indexerParam);
        case it_date:
            return std::make_shared<DateMemIndexer>(indexerParam, invertedIndexMetrics);
        default:
            return std::make_shared<InvertedMemIndexer>(indexerParam, invertedIndexMetrics);
        }
    }
    case InvertedIndexConfig::IndexShardingType::IST_NEED_SHARDING:
        return std::make_shared<MultiShardInvertedMemIndexer>(indexerParam);
    default:
        return nullptr;
    }
}
std::unique_ptr<IIndexReader> InvertedIndexFactory::CreateIndexReader(const std::shared_ptr<IIndexConfig>& indexConfig,
                                                                      const IndexerParameter& indexerParam) const
{
    auto config = std::dynamic_pointer_cast<InvertedIndexConfig>(indexConfig);
    if (!config) {
        AUTIL_LOG(ERROR, "failed to cast IIndexConfig to InvertedIndexConfig, indexName[%s], indexType[%s]",
                  indexConfig->GetIndexName().c_str(), indexConfig->GetIndexType().c_str());
        assert(config);
        return nullptr;
    }
    auto indexType = config->GetInvertedIndexType();
    if (!CheckSupport(indexType)) {
        AUTIL_LOG(ERROR, "create index reader failed, indexType[%s] indexName[%s]", indexConfig->GetIndexType().c_str(),
                  indexConfig->GetIndexName().c_str());
        return nullptr;
    }
    InvertedIndexConfig::IndexShardingType shardingType = config->GetShardingType();
    switch (shardingType) {
    case InvertedIndexConfig::IndexShardingType::IST_NO_SHARDING:
    case InvertedIndexConfig::IndexShardingType::IST_IS_SHARDING: {
        auto invertedIndexMetrics =
            indexerParam.metricsManager ? InvertedIndexMetrics::Create(indexConfig, indexerParam) : nullptr;
        switch (indexType) {
        case it_spatial:
            return std::make_unique<SpatialIndexReader>(invertedIndexMetrics);
        case it_range:
            return std::make_unique<RangeIndexReader>(invertedIndexMetrics);
        case it_date:
            return std::make_unique<DateIndexReader>(invertedIndexMetrics);
        default:
            return std::make_unique<InvertedIndexReaderImpl>(invertedIndexMetrics);
        }
    }
    case InvertedIndexConfig::IndexShardingType::IST_NEED_SHARDING:
        return std::make_unique<MultiShardInvertedIndexReader>(indexerParam);
    default:
        return nullptr;
    }
}
std::unique_ptr<IIndexMerger>
InvertedIndexFactory::CreateIndexMerger(const std::shared_ptr<IIndexConfig>& indexConfig) const
{
    auto config = std::dynamic_pointer_cast<InvertedIndexConfig>(indexConfig);
    if (!config) {
        AUTIL_LOG(ERROR, "failed to cast IIndexConfig to InvertedIndexConfig, indexName[%s], indexType[%s]",
                  indexConfig->GetIndexName().c_str(), indexConfig->GetIndexType().c_str());
        assert(config);
        return nullptr;
    }
    auto indexType = config->GetInvertedIndexType();
    if (!CheckSupport(indexType)) {
        AUTIL_LOG(ERROR, "create index merger failed, indexType[%s] indexName[%s]", indexConfig->GetIndexType().c_str(),
                  indexConfig->GetIndexName().c_str());
        return nullptr;
    }
    switch (indexType) {
    case it_number_int8:
        return std::make_unique<NumberIndexMergerTyped<int8_t, it_number_int8>>();
    case it_number_uint8:
        return std::make_unique<NumberIndexMergerTyped<uint8_t, it_number_uint8>>();
    case it_number_int16:
        return std::make_unique<NumberIndexMergerTyped<int16_t, it_number_int16>>();
    case it_number_uint16:
        return std::make_unique<NumberIndexMergerTyped<uint16_t, it_number_uint16>>();
    case it_number_int32:
        return std::make_unique<NumberIndexMergerTyped<int32_t, it_number_int32>>();
    case it_number_uint32:
        return std::make_unique<NumberIndexMergerTyped<uint32_t, it_number_uint32>>();
    case it_number_int64:
        return std::make_unique<NumberIndexMergerTyped<int64_t, it_number_int64>>();
    case it_number_uint64:
        return std::make_unique<NumberIndexMergerTyped<uint64_t, it_number_uint64>>();
    case it_text:
        return std::make_unique<TextIndexMerger>();
    case it_string:
        return std::make_unique<StringIndexMerger>();
    case it_pack:
        return std::make_unique<PackIndexMerger>();
    case it_expack:
        return std::make_unique<ExpackIndexMerger>();
    case it_spatial:
        return std::make_unique<SpatialIndexMerger>();
    case it_range:
        return std::make_unique<RangeIndexMerger>();
    case it_date:
        return std::make_unique<DateIndexMerger>();
    default:
        break;
    }
    AUTIL_LOG(ERROR, "unsupported inverted index type[%d]", indexType);
    return nullptr;
}

std::unique_ptr<indexlibv2::config::IIndexConfig>
InvertedIndexFactory::CreateIndexConfig(const autil::legacy::Any& any) const
{
    std::string indexName;
    InvertedIndexType invertedIndexType = it_unknown;
    try {
        autil::legacy::Jsonizable::JsonWrapper json(any);
        json.Jsonize("index_name", indexName, indexName);
        if (indexName.empty()) {
            AUTIL_LOG(ERROR, "parse 'index_name' failed, [%s]", autil::legacy::ToJsonString(any, true).c_str());
            return nullptr;
        }
        std::string typeStr;
        json.Jsonize("index_type", typeStr, typeStr);
        if (typeStr.empty()) {
            AUTIL_LOG(ERROR, "parse 'index_type' failed, [%s]", autil::legacy::ToJsonString(any, true).c_str());
            return nullptr;
        }
        auto [status, type] = InvertedIndexConfig::StrToIndexType(typeStr);
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "invalid inverted index type [%s]", typeStr.c_str());
            return nullptr;
        }
        invertedIndexType = type;
    } catch (const std::exception& e) {
        AUTIL_LOG(ERROR, "create index config failed, exception[%s]", e.what());
        return nullptr;
    }
    switch (invertedIndexType) {
    case it_number:
    case it_number_int8:
    case it_number_uint8:
    case it_number_int16:
    case it_number_uint16:
    case it_number_int32:
    case it_number_uint32:
    case it_number_int64:
    case it_number_uint64:
    case it_text:
    case it_string:
        return std::make_unique<indexlibv2::config::SingleFieldIndexConfig>(indexName, invertedIndexType);
    case it_pack:
    case it_expack:
        return std::make_unique<indexlibv2::config::PackageIndexConfig>(indexName, invertedIndexType);
    case it_spatial:
        return std::make_unique<indexlibv2::config::SpatialIndexConfig>(indexName, invertedIndexType);
    case it_range:
        return std::make_unique<indexlibv2::config::RangeIndexConfig>(indexName, invertedIndexType);
    case it_date:
        return std::make_unique<indexlibv2::config::DateIndexConfig>(indexName, invertedIndexType);
    default:
        break;
    }
    AUTIL_LOG(ERROR, "unsupported inverted index type[%d]", invertedIndexType);
    return nullptr;
}

std::string InvertedIndexFactory::GetIndexPath() const { return INVERTED_INDEX_PATH; }

REGISTER_INDEX_FACTORY(inverted_index, InvertedIndexFactory);

__attribute__((constructor)) static void RegisterHooks()
{
    indexlibv2::config::MergeConfig::RegisterOptionHook(
        TRUNCATE_STRATEGY,
        [](std::map<std::string, std::any>& hookOptions) {
            hookOptions.emplace(TRUNCATE_STRATEGY, std::vector<indexlibv2::config::TruncateStrategy>());
        },
        [](autil::legacy::Jsonizable::JsonWrapper& json, std::map<std::string, std::any>& hookOptions) {
            assert(hookOptions.count(TRUNCATE_STRATEGY) > 0);
            assert(hookOptions[TRUNCATE_STRATEGY].type() == typeid(std::vector<indexlibv2::config::TruncateStrategy>));
            auto& truncateStrategyVec =
                std::any_cast<std::vector<indexlibv2::config::TruncateStrategy>&>(hookOptions[TRUNCATE_STRATEGY]);
            json.Jsonize(TRUNCATE_STRATEGY, truncateStrategyVec, truncateStrategyVec);
        });
}

} // namespace indexlib::index
