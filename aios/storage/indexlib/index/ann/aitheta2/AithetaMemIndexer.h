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
#include "indexlib/document/IDocument.h"
#include "indexlib/document/extractor/plain/DocumentInfoExtractorFactory.h"
#include "indexlib/index/IMemIndexer.h"
#include "indexlib/index/MemIndexerParameter.h"
#include "indexlib/index/ann/aitheta2/impl/RealtimeSegmentBuildResource.h"
#include "indexlib/index/ann/aitheta2/impl/RealtimeSegmentSearcher.h"
#include "indexlib/index/ann/aitheta2/impl/SegmentBuilder.h"
#include "indexlib/index/ann/aitheta2/util/IndexFieldParser.h"
#include "indexlib/index/inverted_index/InvertedIndexMetrics.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"

namespace indexlib::index::ann {
class SingleAithetaBuilder;
}

namespace indexlibv2::index::ann {

class AithetaMemIndexer : public IMemIndexer
{
public:
    AithetaMemIndexer(const MemIndexerParameter& indexerParam);
    virtual ~AithetaMemIndexer();

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
    std::string GetIndexName() const override { return _indexName; }
    autil::StringView GetIndexType() const override { return ANN_INDEX_TYPE_STR; }
    const std::shared_ptr<config::ANNIndexConfig> GetIndexConfig() const { return _annIndexConfig; }
    void Seal() override;
    bool IsDirty() const override;

    void SetBaseDocId(docid_t baseDocId) { _baseDocId = baseDocId; }
    std::pair<Status, std::shared_ptr<RealtimeSegmentSearcher>>
    CreateSearcher(docid_t segmentBaseDocId, const std::shared_ptr<AithetaFilterCreator>& creator);
    Status AddDocument(indexlib::document::IndexDocument* doc);

private:
    bool IsFullBuildPhase();
    std::shared_ptr<RealtimeSegmentBuildResource> CreateRealtimeBuildResource();
    Status InitRealtimeSegmentBuilder();
    Status InitNormalSegmentBuilder();

private:
    Status BuildSingleDoc(document::IDocument* doc);
    bool ShouldSkipBuild() const;
    friend class indexlib::index::ann::SingleAithetaBuilder;

private:
    std::string _indexName;
    AithetaIndexConfig _aithetaIndexConfig;
    MemIndexerParameter _indexerParam;
    std::shared_ptr<config::ANNIndexConfig> _annIndexConfig;
    int32_t _docCount = 0;
    int32_t _invalidDocCount = 0;

    std::unique_ptr<document::extractor::IDocumentInfoExtractor> _docInfoExtractor;
    std::unique_ptr<IndexFieldParser> _fieldParser;
    std::shared_ptr<SegmentBuilder> _segmentBuilder;
    docid_t _baseDocId = INVALID_DOCID;

    MetricReporterPtr _metricReporter;
    MetricPtr _buildLatencyMetric;
    MetricPtr _dumpLatencyMetric;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index::ann
