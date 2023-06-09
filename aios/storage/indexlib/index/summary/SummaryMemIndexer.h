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
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/index/IMemIndexer.h"

namespace indexlib::document {
class SerializedSummaryDocument;
}
namespace indexlib::index {
class SingleSummaryBuilder;
}

namespace indexlibv2 {
namespace config {
class SummaryIndexConfig;
}
namespace document {
class IDocument;
namespace extractor {
class IDocumentInfoExtractor;
}
} // namespace document

namespace framework {
struct DumpParams;
}
} // namespace indexlibv2

namespace indexlibv2::index {

#define DECLARE_SUMMARY_MEM_INDEXER_IDENTIFIER(id)                                                                     \
    static std::string Identifier() { return std::string("summary.mem.indexer." #id); }                                \
    virtual std::string GetIdentifier() const { return Identifier(); }

class SummaryMemReader;
class SummaryMemReaderContainer;
class LocalDiskSummaryMemIndexer;
class SummaryMemIndexer : public IMemIndexer
{
public:
    SummaryMemIndexer() = default;
    virtual ~SummaryMemIndexer() = default;
    DECLARE_SUMMARY_MEM_INDEXER_IDENTIFIER(base);

public:
    virtual Status Init(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                        document::extractor::IDocumentInfoExtractorFactory* docInfoExtractorFactory) override;
    virtual Status Build(document::IDocumentBatch* docBatch) override;
    virtual Status Build(const document::IIndexFields* indexFields, size_t n) override;
    virtual Status Dump(autil::mem_pool::PoolBase* dumpPool,
                        const std::shared_ptr<indexlib::file_system::Directory>& indexDirectory,
                        const std::shared_ptr<framework::DumpParams>& params) override;
    virtual void ValidateDocumentBatch(document::IDocumentBatch* docBatch) override;
    virtual bool IsValidDocument(document::IDocument* doc) override;
    virtual bool IsValidField(const document::IIndexFields* fields) override;
    virtual void FillStatistics(const std::shared_ptr<indexlib::framework::SegmentMetrics>& segmentMetrics) override {}
    virtual void UpdateMemUse(BuildingIndexMemoryUseUpdater* memUpdater) override;
    virtual std::string GetIndexName() const override;
    virtual autil::StringView GetIndexType() const override;
    virtual void Seal() override {}

    bool IsDirty() const override;

public:
    const std::shared_ptr<SummaryMemReaderContainer> CreateInMemReader();

public:
    static bool ShouldSkipBuild(config::SummaryIndexConfig* summaryIndexConfig);

private:
    Status AddDocument(document::IDocument* doc);
    friend class indexlib::index::SingleSummaryBuilder;

protected:
    std::vector<std::shared_ptr<LocalDiskSummaryMemIndexer>> _groupMemIndexers;
    std::shared_ptr<config::SummaryIndexConfig> _summaryIndexConfig;
    std::unique_ptr<document::extractor::IDocumentInfoExtractor> _docInfoExtractor;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
