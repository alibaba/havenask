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
#include "indexlib/index/ann/ANNIndexFactory.h"

#include "indexlib/index/ann/ANNIndexConfig.h"
#include "indexlib/index/ann/Common.h"
#include "indexlib/index/ann/aitheta2/AithetaDiskIndexer.h"
#include "indexlib/index/ann/aitheta2/AithetaIndexMerger.h"
#include "indexlib/index/ann/aitheta2/AithetaIndexReader.h"
#include "indexlib/index/ann/aitheta2/AithetaMemIndexer.h"

namespace indexlibv2::index {

AUTIL_LOG_SETUP(indexlib.index, ANNIndexFactory);

std::shared_ptr<IDiskIndexer>
ANNIndexFactory::CreateDiskIndexer(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                   const IndexerParameter& indexerParam) const
{
    return std::make_shared<ann::AithetaDiskIndexer>(indexerParam);
}

std::shared_ptr<IMemIndexer> ANNIndexFactory::CreateMemIndexer(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                                               const IndexerParameter& indexerParam) const
{
    auto annIndexConfig = std::dynamic_pointer_cast<config::ANNIndexConfig>(indexConfig);
    if (!annIndexConfig) {
        AUTIL_SLOG(ERROR) << "expect ANNIndexConfig, actual: " << indexConfig->GetIndexType();
        return nullptr;
    }
    return std::make_shared<ann::AithetaMemIndexer>(indexerParam);
}

std::unique_ptr<IIndexReader>
ANNIndexFactory::CreateIndexReader(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                   const IndexerParameter& indexerParam) const
{
    return std::make_unique<ann::AithetaIndexReader>(indexerParam);
}

std::unique_ptr<IIndexMerger>
ANNIndexFactory::CreateIndexMerger(const std::shared_ptr<config::IIndexConfig>& indexConfig) const
{
    return std::make_unique<ann::AithetaIndexMerger>();
}

std::unique_ptr<config::IIndexConfig> ANNIndexFactory::CreateIndexConfig(const autil::legacy::Any& any) const
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
        auto [status, type] = config::InvertedIndexConfig::StrToIndexType(typeStr);
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "invalid inverted index type [%s]", typeStr.c_str());
            return nullptr;
        }
        invertedIndexType = type;
    } catch (const std::exception& e) {
        AUTIL_LOG(ERROR, "create index config failed, exception[%s]", e.what());
        return nullptr;
    }
    return std::make_unique<config::ANNIndexConfig>(indexName, invertedIndexType);
}

std::string ANNIndexFactory::GetIndexPath() const { return ANN_INDEX_PATH; }

REGISTER_INDEX_FACTORY(ann, ANNIndexFactory);

} // namespace indexlibv2::index
