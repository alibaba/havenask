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
#pragma once

#include <map>
#include <memory>

#include "autil/Log.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/index/IndexerParameter.h"
#include "indexlib/index/inverted_index/IInvertedDiskIndexer.h"

namespace indexlib::file_system {
class Directory;
}

namespace indexlibv2::index {
template <typename T>
class MultiValueAttributeDiskIndexer;
}

namespace indexlibv2::config {
class IIndexConfig;
class AttributeConfig;
class InvertedIndexConfig;
} // namespace indexlibv2::config

namespace indexlib::index {
class InvertedDiskIndexer;
class ShardingIndexHasher;

class MultiShardInvertedDiskIndexer : public IInvertedDiskIndexer
{
public:
    MultiShardInvertedDiskIndexer(const indexlibv2::index::IndexerParameter& indexerParam);
    ~MultiShardInvertedDiskIndexer() = default;

    Status Open(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                const std::shared_ptr<file_system::IDirectory>& indexDirectory) override;
    size_t EstimateMemUsed(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                           const std::shared_ptr<file_system::IDirectory>& indexDirectory) override;
    size_t EvaluateCurrentMemUsed() override;

    void UpdateTokens(docid_t localDocId, const document::ModifiedTokens& modifiedTokens) override;
    void UpdateTokens(docid_t localDocId, const document::ModifiedTokens& modifiedTokens, size_t shardId);

    void UpdateTerms(IndexUpdateTermIterator* termIter) override;
    void UpdateBuildResourceMetrics(size_t& poolMemory, size_t& totalMemory, size_t& totalRetiredMemory,
                                    size_t& totalDocCount, size_t& totalAlloc, size_t& totalFree,
                                    size_t& totalTreeCount) const override;

public:
    std::shared_ptr<InvertedDiskIndexer> GetSingleShardIndexer(const std::string& indexName) const;
    std::shared_ptr<IDiskIndexer> GetSectionAttributeDiskIndexer() const override;

    std::shared_ptr<IDiskIndexer> GetMultiSliceAttributeDiskIndexer() const { return _multiSliceAttrDiskIndexer; }

private:
    size_t EstimateSectionAttributeMemUsed(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& config,
                                           const std::shared_ptr<file_system::IDirectory>& indexDirectory);

private:
    indexlibv2::index::IndexerParameter _indexerParam;
    std::map<std::string, std::shared_ptr<InvertedDiskIndexer>> _indexers;
    std::vector<std::shared_ptr<InvertedDiskIndexer>> _shardIndexers;
    std::unique_ptr<ShardingIndexHasher> _indexHasher;
    std::shared_ptr<indexlibv2::index::MultiValueAttributeDiskIndexer<char>> _sectionAttrDiskIndexer;
    std::shared_ptr<IDiskIndexer> _multiSliceAttrDiskIndexer;
    std::shared_ptr<indexlibv2::config::AttributeConfig> _sectionAttrConfig;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
