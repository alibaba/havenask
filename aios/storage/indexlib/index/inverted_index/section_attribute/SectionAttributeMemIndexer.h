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
#include "indexlib/index/IMemIndexer.h"
#include "indexlib/index/attribute/MultiValueAttributeMemIndexer.h"

namespace indexlibv2::config {
class PackageIndexConfig;
}
namespace indexlib::document {
class IndexDocument;
}
namespace indexlibv2::index {
class InvertedIndexFields;
}

namespace indexlib::index {

class SectionAttributeMemIndexer : public indexlibv2::index::IMemIndexer
{
public:
    SectionAttributeMemIndexer(const indexlibv2::index::MemIndexerParameter& indexerParam);
    ~SectionAttributeMemIndexer();

public:
    Status Init(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                indexlibv2::document::extractor::IDocumentInfoExtractorFactory* docInfoExtractorFactory) override;
    Status Build(indexlibv2::document::IDocumentBatch* docBatch) override;
    Status Build(const indexlibv2::document::IIndexFields* indexFields, size_t n) override;
    Status Dump(autil::mem_pool::PoolBase* dumpPool, const std::shared_ptr<file_system::Directory>& indexDirectory,
                const std::shared_ptr<indexlibv2::framework::DumpParams>& dumpParams) override;
    void ValidateDocumentBatch(indexlibv2::document::IDocumentBatch* docBatch) override;
    bool IsValidDocument(indexlibv2::document::IDocument* doc) override
    {
        assert(false);
        return true;
    }
    bool IsValidField(const indexlibv2::document::IIndexFields* fields) override;
    void FillStatistics(const std::shared_ptr<framework::SegmentMetrics>& segmentMetrics) override {}
    void UpdateMemUse(indexlibv2::index::BuildingIndexMemoryUseUpdater* memUpdater) override;
    std::string GetIndexName() const override;
    autil::StringView GetIndexType() const override;
    void Seal() override { _attrMemIndexer->Seal(); }
    bool IsDirty() const override;

    void EndDocument(const document::IndexDocument& indexDocument);
    void EndDocument(const indexlibv2::index::InvertedIndexFields* indexFields, docid64_t docId) { assert(false); }

    const std::shared_ptr<indexlibv2::index::AttributeMemReader> CreateInMemReader() const
    {
        return _attrMemIndexer->CreateInMemReader();
    }

private:
    std::shared_ptr<indexlibv2::config::PackageIndexConfig> _config;
    std::unique_ptr<indexlibv2::index::MultiValueAttributeMemIndexer<char>> _attrMemIndexer;
    indexlibv2::index::MemIndexerParameter _indexerParam;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
