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

#include "autil/Log.h"
#include "indexlib/index/MemIndexerParameter.h"
#include "indexlib/index/inverted_index/IInvertedMemIndexer.h"
#include "indexlib/index/inverted_index/Types.h"

namespace indexlib::document {
class IndexDocument;
class Field;
class Token;
} // namespace indexlib::document

namespace indexlibv2::config {
class InvertedIndexConfig;
}
namespace indexlib::index {
class ShardingIndexHasher;
class IndexFormatOption;
class InvertedMemIndexer;
class SectionAttributeMemIndexer;

// 多线程Build时可以指定只处理某个shard的数据，当shardId=INVALID_SHARDID时默认处理所有shard的数据
class MultiShardInvertedMemIndexer : public IInvertedMemIndexer
{
public:
    using IndexerAndMemUpdaterPair = std::pair<std::shared_ptr<InvertedMemIndexer>,
                                               std::shared_ptr<indexlibv2::index::BuildingIndexMemoryUseUpdater>>;

    explicit MultiShardInvertedMemIndexer(const indexlibv2::index::MemIndexerParameter& indexerParam);
    ~MultiShardInvertedMemIndexer();

    Status Init(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                indexlibv2::document::extractor::IDocumentInfoExtractorFactory* docInfoExtractorFactory) override;
    Status Build(indexlibv2::document::IDocumentBatch* docBatch) override;
    Status Build(const indexlibv2::document::IIndexFields* indexFields, size_t n) override;
    Status AddDocument(document::IndexDocument* doc) override;
    Status AddDocument(document::IndexDocument* doc, size_t shardId);
    Status Dump(autil::mem_pool::PoolBase* dumpPool, const std::shared_ptr<file_system::Directory>& indexDirectory,
                const std::shared_ptr<indexlibv2::framework::DumpParams>& dumpParams) override;
    void ValidateDocumentBatch(indexlibv2::document::IDocumentBatch* docBatch) override;
    bool IsValidDocument(indexlibv2::document::IDocument* doc) override;
    bool IsValidField(const indexlibv2::document::IIndexFields* fields) override;
    void FillStatistics(const std::shared_ptr<framework::SegmentMetrics>& segmentMetrics) override;
    void UpdateMemUse(indexlibv2::index::BuildingIndexMemoryUseUpdater* updater) override;
    std::string GetIndexName() const override;
    autil::StringView GetIndexType() const override;
    const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& GetIndexConfig() const override
    {
        return _indexConfig;
    }
    void Seal() override;
    std::shared_ptr<InvertedMemIndexer> GetSingleShardIndexer(const std::string& indexName) const;
    bool IsDirty() const override;

    void UpdateTokens(docid_t docId, const document::ModifiedTokens& modifiedTokens) override;
    void UpdateTokens(docid_t docId, const document::ModifiedTokens& modifiedTokens, size_t shardId);

    std::shared_ptr<InvertedIndexMetrics> GetMetrics() const override;

    std::shared_ptr<indexlibv2::index::AttributeMemReader> CreateSectionAttributeMemReader() const override;

public:
    indexlibv2::document::extractor::IDocumentInfoExtractor* GetDocInfoExtractor() const override;

private:
    Status AddField(const document::Field* field, size_t shardId, pos_t* basePos);
    Status AddToken(const document::Token* token, fieldid_t fieldId, pos_t tokenBasePos, size_t shardId);

    void EndDocument(const document::IndexDocument& indexDocument);
    void EndDocument(const document::IndexDocument& indexDocument, size_t shardId);
    Status DocBatchToDocs(indexlibv2::document::IDocumentBatch* docBatch,
                          std::vector<document::IndexDocument*>* docs) const;

private:
    std::vector<fieldid_t> _fieldIds; // field ids related to this indexer
    int32_t _docCount = 0;
    indexlibv2::index::MemIndexerParameter _indexerParam;
    std::shared_ptr<indexlibv2::config::InvertedIndexConfig> _indexConfig;

    std::vector<IndexerAndMemUpdaterPair> _memIndexers;
    std::unique_ptr<ShardingIndexHasher> _indexHasher;
    std::unique_ptr<SectionAttributeMemIndexer> _sectionAttributeMemIndexer;
    std::shared_ptr<IndexFormatOption> _indexFormatOption;
    std::unique_ptr<indexlibv2::document::extractor::IDocumentInfoExtractor> _docInfoExtractor;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
