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
#include "indexlib/index/inverted_index/InvertedMemIndexer.h"

namespace indexlibv2::config {
class InvertedIndexConfig;
class RangeIndexConfig;
} // namespace indexlibv2::config

namespace indexlib::document {
class IndexDocument;
class Field;
} // namespace indexlib::document

namespace indexlib::index {
class RangeInfo;
class InvertedIndexMetrics;
class IndexSegmentReader;
class RangeMemIndexer : public InvertedMemIndexer
{
public:
    explicit RangeMemIndexer(const indexlibv2::index::IndexerParameter& indexerParam);
    ~RangeMemIndexer();

    Status Init(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                indexlibv2::document::extractor::IDocumentInfoExtractorFactory* docInfoExtractorFactory) override;
    void FillStatistics(const std::shared_ptr<framework::SegmentMetrics>& segmentMetrics) override;
    void UpdateMemUse(indexlibv2::index::BuildingIndexMemoryUseUpdater* memUpdater) override;
    void Seal() override;
    std::shared_ptr<IndexSegmentReader> CreateInMemReader() override;
    Status AddField(const document::Field* field) override;
    void EndDocument(const document::IndexDocument& indexDocument) override;

protected:
    Status DoDump(autil::mem_pool::PoolBase* dumpPool, const std::shared_ptr<file_system::Directory>& indexDirectory,
                  const std::shared_ptr<indexlibv2::framework::DumpParams>& dumpParams) override;

private:
    void SetDistinctTermCount(const std::string& indexName,
                              const std::shared_ptr<framework::SegmentMetrics>& segmentMetrics);
    std::shared_ptr<InvertedMemIndexer>
    CreateInvertedMemIndexer(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig);

private:
    std::shared_ptr<RangeInfo> _rangeInfo;
    std::shared_ptr<InvertedMemIndexer> _bottomLevelMemIndexer;
    std::shared_ptr<indexlibv2::index::BuildingIndexMemoryUseUpdater> _bottomLevelMemUpdater;
    std::shared_ptr<InvertedMemIndexer> _highLevelMemIndexer;
    std::shared_ptr<indexlibv2::index::BuildingIndexMemoryUseUpdater> _highLevelMemUpdater;
    std::shared_ptr<indexlibv2::config::RangeIndexConfig> _rangeIndexConfig;

    static constexpr double DEFAULT_BOTTOM_LEVEL_TERM_COUNT_RATIO = 0.8;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
