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
#include "autil/NoCopyable.h"
#include "indexlib/base/Types.h"
#include "indexlib/document/extractor/IDocumentInfoExtractor.h"
#include "indexlib/index/IMemIndexer.h"
#include "indexlib/index/source/Types.h"

namespace indexlibv2::config {
class SourceIndexConfig;
} // namespace indexlibv2::config
namespace indexlib::document {
class SerializedSourceDocument;
} // namespace indexlib::document
namespace indexlib::index {
class SingleSourceBuilder;
}

namespace indexlib::util {
class BuildResourceMetrics;
}

namespace indexlibv2::index {
class SourceGroupWriter;
class GroupFieldDataWriter;
struct MemIndexerParameter;
class SingleSourceBuilder;
class SourceMemIndexer : public IMemIndexer
{
public:
    explicit SourceMemIndexer(const indexlibv2::index::MemIndexerParameter& indexerParam);
    ~SourceMemIndexer() = default;

public:
    Status Init(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                document::extractor::IDocumentInfoExtractorFactory* docInfoExtractorFactory) override;
    Status Build(document::IDocumentBatch* docBatch) override;
    Status Build(const document::IIndexFields* indexFields, size_t n) override;
    Status Dump(autil::mem_pool::PoolBase* dumpPool,
                const std::shared_ptr<indexlib::file_system::Directory>& indexDirectory,
                const std::shared_ptr<framework::DumpParams>& params) override;
    void ValidateDocumentBatch(document::IDocumentBatch* docBatch) override;
    bool IsValidDocument(document::IDocument* doc) override;
    bool IsValidField(const document::IIndexFields* fields) override;
    void FillStatistics(const std::shared_ptr<indexlib::framework::SegmentMetrics>& segmentMetrics) override;
    void UpdateMemUse(BuildingIndexMemoryUseUpdater* memUpdater) override;
    std::string GetIndexName() const override;
    autil::StringView GetIndexType() const override;
    void Seal() override;
    bool IsDirty() const override;

    bool GetDocument(docid_t docId, const std::vector<index::sourcegroupid_t>& requiredGroupdIds,
                     indexlib::document::SerializedSourceDocument* sourceDocument) const;

private:
    Status AddDocument(document::IDocument* doc);
    friend class indexlib::index::SingleSourceBuilder;

private:
    indexlib::util::BuildResourceMetrics* _buildResourceMetrics;
    std::shared_ptr<config::SourceIndexConfig> _config;
    std::shared_ptr<GroupFieldDataWriter> _metaWriter;
    using IndexerAndMemUpdaterPair = std::pair<std::shared_ptr<SourceGroupWriter>,
                                               std::shared_ptr<indexlibv2::index::BuildingIndexMemoryUseUpdater>>;
    std::vector<IndexerAndMemUpdaterPair> _dataWriters;
    std::unique_ptr<document::extractor::IDocumentInfoExtractor> _docInfoExtractor;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
