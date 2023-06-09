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
#include "indexlib/index/primary_key/PrimaryKeyIndexFactory.h"

#include "indexlib/index/IndexerParameter.h"
#include "indexlib/index/primary_key/Common.h"
#include "indexlib/index/primary_key/PrimaryKeyReader.h"
#include "indexlib/index/primary_key/PrimaryKeyWriter.h"
#include "indexlib/index/primary_key/config/PrimaryKeyIndexConfig.h"
#include "indexlib/index/primary_key/merger/PrimaryKeyMerger.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"

using namespace std;

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, PrimaryKeyIndexFactory);

PrimaryKeyIndexFactory::PrimaryKeyIndexFactory() {}

PrimaryKeyIndexFactory::~PrimaryKeyIndexFactory() {}

InvertedIndexType PrimaryKeyIndexFactory::GetInvertedIndexType(const std::shared_ptr<config::IIndexConfig>& indexConfig)
{
    auto invertedIndexConfig = std::dynamic_pointer_cast<const indexlibv2::config::InvertedIndexConfig>(indexConfig);
    assert(invertedIndexConfig);
    auto indexType = invertedIndexConfig->GetInvertedIndexType();
    assert(indexType == it_primarykey64 || indexType == it_primarykey128);
    return indexType;
}

std::shared_ptr<IDiskIndexer>
PrimaryKeyIndexFactory::CreateDiskIndexer(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                          const IndexerParameter& indexerParam) const
{
    auto indexType = GetInvertedIndexType(indexConfig);
    if (indexType == it_primarykey64) {
        return std::make_shared<index::PrimaryKeyDiskIndexer<uint64_t>>(indexerParam);
    }
    if (indexType == it_primarykey128) {
        return std::make_shared<index::PrimaryKeyDiskIndexer<autil::uint128_t>>(indexerParam);
    }
    assert(false);
    return nullptr;
}
std::shared_ptr<IMemIndexer>
PrimaryKeyIndexFactory::CreateMemIndexer(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                         const IndexerParameter& indexerParam) const
{
    auto indexType = GetInvertedIndexType(indexConfig);
    if (indexType == it_primarykey64) {
        return std::make_shared<index::PrimaryKeyWriter<uint64_t>>(indexerParam.buildResourceMetrics);
    }
    if (indexType == it_primarykey128) {
        return std::make_shared<index::PrimaryKeyWriter<autil::uint128_t>>(indexerParam.buildResourceMetrics);
    }
    assert(false);
    return nullptr;
}
std::unique_ptr<IIndexReader>
PrimaryKeyIndexFactory::CreateIndexReader(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                          const IndexerParameter& indexerParam) const
{
    auto indexType = GetInvertedIndexType(indexConfig);
    if (indexType == it_primarykey64) {
        return std::make_unique<index::PrimaryKeyReader<uint64_t>>(indexerParam.metricsManager);
    }
    if (indexType == it_primarykey128) {
        return std::make_unique<index::PrimaryKeyReader<autil::uint128_t>>(indexerParam.metricsManager);
    }
    assert(false);
    return nullptr;
}
std::unique_ptr<IIndexMerger>
PrimaryKeyIndexFactory::CreateIndexMerger(const std::shared_ptr<config::IIndexConfig>& indexConfig) const
{
    auto indexType = GetInvertedIndexType(indexConfig);
    if (indexType == it_primarykey64) {
        return std::make_unique<index::PrimaryKeyMerger<uint64_t>>();
    }
    if (indexType == it_primarykey128) {
        return std::make_unique<index::PrimaryKeyMerger<autil::uint128_t>>();
    }
    assert(false);
    return nullptr;
}

std::unique_ptr<config::IIndexConfig> PrimaryKeyIndexFactory::CreateIndexConfig(const autil::legacy::Any& any) const
{
    auto jsonMap = autil::legacy::AnyCast<autil::legacy::json::JsonMap>(any);
    std::string indexName;
    auto iter = jsonMap.find("index_name");
    if (iter != jsonMap.end()) {
        indexName = autil::legacy::AnyCast<std::string>(iter->second);
    }
    if (indexName.empty()) {
        AUTIL_LOG(ERROR, "parse 'index_name' failed");
        return nullptr;
    }
    InvertedIndexType indexType = it_unknown;
    iter = jsonMap.find("index_type");
    if (iter != jsonMap.end()) {
        auto [status, type] =
            config::InvertedIndexConfig::StrToIndexType(autil::legacy::AnyCast<std::string>(iter->second));
        if (status.IsOK()) {
            indexType = type;
        }
    }
    if (indexType == it_unknown) {
        AUTIL_LOG(ERROR, "parse 'index_type' failed");
        return nullptr;
    }
    return std::make_unique<index::PrimaryKeyIndexConfig>(indexName, indexType);
}

std::string PrimaryKeyIndexFactory::GetIndexPath() const { return "index"; }

REGISTER_INDEX(primarykey, PrimaryKeyIndexFactory);

} // namespace indexlibv2::index
