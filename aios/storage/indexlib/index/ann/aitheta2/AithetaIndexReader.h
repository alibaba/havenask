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
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/index/IndexerParameter.h"
#include "indexlib/index/ann/aitheta2/AithetaFilterCreator.h"
#include "indexlib/index/ann/aitheta2/AithetaIndexConfig.h"
#include "indexlib/index/ann/aitheta2/impl/NormalSegmentSearcher.h"
#include "indexlib/index/ann/aitheta2/impl/RealtimeSegmentSearcher.h"
#include "indexlib/index/ann/aitheta2/util/MetricReporter.h"
#include "indexlib/index/inverted_index/InvertedIndexReader.h"

namespace indexlibv2::index::ann {
class AithetaRecallReporter;
} // namespace indexlibv2::index::ann

namespace indexlibv2::index::ann {
class AithetaIndexReader : public indexlib::index::InvertedIndexReader
{
public:
    AithetaIndexReader(const IndexerParameter& indexerParam);
    ~AithetaIndexReader();
    friend class AithetaRecallReporter;

public:
    Status Open(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                const indexlibv2::framework::TabletData* tabletData) override;

    indexlib::index::Result<indexlib::index::PostingIterator*>
    Lookup(const indexlib::index::Term& term, uint32_t inDocPositionStatePoolSize = DEFAULT_STATE_POOL_SIZE,
           PostingType type = pt_default, autil::mem_pool::Pool* sessionPool = nullptr) override;
    future_lite::coro::Lazy<indexlib::index::Result<indexlib::index::PostingIterator*>>
    LookupAsync(const indexlib::index::Term* term, uint32_t statePoolSize, PostingType type,
                autil::mem_pool::Pool* pool, indexlib::file_system::ReadOption option) noexcept override
    {
        co_return Lookup(*term, statePoolSize, type, pool);
    }
    const indexlib::index::SectionAttributeReader* GetSectionReader(const std::string& indexName) const override
    {
        assert(false);
        return nullptr;
    };
    std::shared_ptr<indexlib::index::KeyIterator> CreateKeyIterator(const std::string& indexName) override
    {
        assert(false);
        return nullptr;
    };

protected:
    bool hasRtSearcher() const { return !_realtimeSearchers.empty(); }
    docid_t GetLatestRtBaseDocId() const { return _latestRtBaseDocId; }
    Status DoSearch(const AithetaQueries& indexQuery, const std::shared_ptr<AithetaAuxSearchInfoBase>& searchInfo,
                    ResultHolder& resultHolder, bool searchRtOnly = false) const;

private:
    Status InitMetrics(const std::string& indexName);
    Status AddNormalSearcher(std::shared_ptr<IIndexer>& indexer, docid_t segmentBaseDocId,
                             const std::shared_ptr<AithetaFilterCreator>& creator);
    Status AddRealtimeSearcher(std::shared_ptr<IIndexer>& indexer, docid_t segmentBaseDocId,
                               const std::shared_ptr<AithetaFilterCreator>& creator);
    Status ParseQuery(const indexlib::index::Term& term, AithetaQueries& indexQuery,
                      std::shared_ptr<AithetaAuxSearchInfoBase>& searchInfo);

private:
    bool GetSegmentPosting(const indexlib::index::DictKeyInfo& key, uint32_t segmentIdx,
                           indexlib::index::SegmentPosting& segPosting, indexlib::file_system::ReadOption option,
                           indexlib::index::InvertedIndexSearchTracer* tracer) override
    {
        assert(false);
        return false;
    }
    future_lite::coro::Lazy<indexlib::index::Result<bool>>
    GetSegmentPostingAsync(const indexlib::index::DictKeyInfo& key, uint32_t segmentIdx,
                           indexlib::index::SegmentPosting& segPosting, indexlib::file_system::ReadOption option,
                           indexlib::index::InvertedIndexSearchTracer* tracer) noexcept override
    {
        assert(false);
        co_return false;
    }
    void SetAccessoryReader(const std::shared_ptr<indexlib::index::IndexAccessoryReader>& accessorReader) override {}

private:
    IndexerParameter _indexerParam;
    AithetaIndexConfig _aithetaIndexConfig;
    std::vector<std::shared_ptr<NormalSegmentSearcher>> _normalSearchers;
    std::vector<std::shared_ptr<RealtimeSegmentSearcher>> _realtimeSearchers;
    std::shared_ptr<AithetaRecallReporter> _recallReporter;
    docid_t _latestRtBaseDocId;

    MetricReporterPtr _metricReporter;
    MetricPtr _searchCountMetric;
    MetricPtr _searchLatencyMetric;
    MetricPtr _searchQpsMetric;
    MetricPtr _filteredCountMetric;
    MetricPtr _distCalcCountMetric;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index::ann
