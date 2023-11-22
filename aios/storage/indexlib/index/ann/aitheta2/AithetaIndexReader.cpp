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
#include "indexlib/index/ann/aitheta2/AithetaIndexReader.h"

#include "indexlib/framework/MetricsManager.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/index/ann/ANNPostingIterator.h"
#include "indexlib/index/ann/aitheta2/AithetaDiskIndexer.h"
#include "indexlib/index/ann/aitheta2/AithetaMemIndexer.h"
#include "indexlib/index/ann/aitheta2/AithetaRecallReporter.h"
#include "indexlib/index/ann/aitheta2/impl/CustomizedAithetaLogger.h"
#include "indexlib/index/ann/aitheta2/util/QueryParser.h"
#include "indexlib/index/deletionmap/DeletionMapIndexReader.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"

using namespace std;
using namespace indexlib::index;
using namespace indexlibv2::framework;

namespace indexlibv2::index::ann {
AUTIL_LOG_SETUP(indexlib.index, AithetaIndexReader);

AithetaIndexReader::AithetaIndexReader(const IndexReaderParameter& indexReaderParam)
    : _indexReaderParam(indexReaderParam)
    , _recallReporter(nullptr)
    , _latestRtBaseDocId(INVALID_DOCID)
    , _metricReporter(nullptr)
{
}

AithetaIndexReader::~AithetaIndexReader() {}

Status AithetaIndexReader::Open(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                                const indexlibv2::framework::TabletData* tabletData)
{
    auto invertedIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::InvertedIndexConfig>(indexConfig);
    if (invertedIndexConfig == nullptr) {
        RETURN_STATUS_ERROR(InvalidArgs, "fail cast to inverted index config");
    }
    _indexConfig = invertedIndexConfig;
    CustomizedAiThetaLogger::RegisterIndexLogger();
    auto annIndexConfig = std::dynamic_pointer_cast<config::ANNIndexConfig>(indexConfig);
    if (annIndexConfig == nullptr) {
        RETURN_STATUS_ERROR(InvalidArgs, "fail cast to ann index config");
    }
    _aithetaIndexConfig = AithetaIndexConfig(annIndexConfig->GetParameters());
    auto [status, deletionMapReader] = DeletionMapIndexReader::Create(tabletData);
    RETURN_IF_STATUS_ERROR(status, "open deletion map reader failed");

    std::shared_ptr<indexlibv2::index::DeletionMapIndexReader> sharedDelReader(std::move(deletionMapReader));
    auto creator = std::make_shared<AithetaFilterCreator>(sharedDelReader);
    const string& indexName = indexConfig->GetIndexName();
    docid_t baseDocId = 0;
    auto segments = tabletData->CreateSlice();
    size_t totalIndexDocCount = 0;
    std::tie(status, totalIndexDocCount) = GetTotalIndexDocCount(indexConfig, tabletData);
    RETURN_IF_STATUS_ERROR(status, "get total index doc count failed");

    for (const auto& segment : segments) {
        segmentid_t segmenId = segment->GetSegmentId();
        uint64_t docCount = segment->GetSegmentInfo()->GetDocCount();
        auto segmentStatus = segment->GetSegmentStatus();
        if (segmentStatus == indexlibv2::framework::Segment::SegmentStatus::ST_BUILT && docCount == 0) {
            AUTIL_LOG(INFO, "segment [%d] has no doc count, open [%s] do nothing", segmenId, indexName.c_str());
            continue;
        }
        auto [status, indexer] = segment->GetIndexer(indexConfig->GetIndexType(), indexConfig->GetIndexName());
        RETURN_IF_STATUS_ERROR(status, "no indexer for [%s] in segment [%d]", indexName.c_str(), segmenId);
        if (segmentStatus == indexlibv2::framework::Segment::SegmentStatus::ST_BUILT) {
            status = AddNormalSearcher(indexer, baseDocId, totalIndexDocCount, creator);
            RETURN_IF_STATUS_ERROR(status, "add normal searcher failed");
        } else {
            if (!_aithetaIndexConfig.realtimeConfig.enable) {
                AUTIL_LOG(INFO, "segment [%d] not need add realtime searcher", segmenId);
                continue;
            }
            status = AddRealtimeSearcher(indexer, baseDocId, creator);
            RETURN_IF_STATUS_ERROR(status, "add realtime searcher failed");
            _latestRtBaseDocId = baseDocId > _latestRtBaseDocId ? baseDocId : _latestRtBaseDocId;
        }
        baseDocId += docCount;
    }
    initTokenHasher(annIndexConfig);

    RETURN_IF_STATUS_ERROR(InitMetrics(indexName), "init metrics failed.");
    AUTIL_LOG(INFO, "open index reader with built segment count[%lu] and realtime segment count[%lu]",
              _normalSearchers.size(), _realtimeSearchers.size());
    return Status::OK();
}

std::pair<Status, size_t>
AithetaIndexReader::GetTotalIndexDocCount(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                                          const indexlibv2::framework::TabletData* tabletData)
{
    size_t indexDocCount = 0;
    auto segments = tabletData->CreateSlice();
    for (const auto& segment : segments) {
        if (segment->GetSegmentStatus() != indexlibv2::framework::Segment::SegmentStatus::ST_BUILT ||
            segment->GetSegmentInfo()->GetDocCount() == 0) {
            continue;
        }
        auto [status, indexer] = segment->GetIndexer(indexConfig->GetIndexType(), indexConfig->GetIndexName());
        if (!status.IsOK()) {
            return {status, 0};
        }
        auto diskIndexer = std::dynamic_pointer_cast<AithetaDiskIndexer>(indexer);
        if (nullptr == diskIndexer) {
            return {Status::InternalError("cast to AithetaDiskIndexer failed"), 0};
        }
        auto indexSegment = diskIndexer->GetSegment();
        if (indexSegment == nullptr) {
            continue;
        }
        indexDocCount += indexSegment->GetSegmentMeta().GetDocCount();
    }
    AUTIL_LOG(INFO, "index [%s] has total index doc count [%lu]", indexConfig->GetIndexName().c_str(), indexDocCount);
    return {Status::OK(), indexDocCount};
}

indexlib::index::Result<PostingIterator*> AithetaIndexReader::Lookup(const indexlib::index::Term& term,
                                                                     uint32_t inDocPositionStatePoolSize,
                                                                     PostingType type,
                                                                     autil::mem_pool::Pool* sessionPool)
{
    // TODO(peaker.lgf): add qps and latency metric
    QPS_REPORT(_searchQpsMetric);
    ScopedLatencyReporter reporter(_searchLatencyMetric);

    if (_normalSearchers.empty() && _realtimeSearchers.empty()) {
        auto annPostingIter = IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, ANNPostingIterator, sessionPool);
        return indexlib::index::Result<PostingIterator*>(annPostingIter);
    }

    AithetaQueries indexQuery;
    std::shared_ptr<AithetaAuxSearchInfoBase> searchInfo;
    auto status = ParseQuery(term, indexQuery, searchInfo);
    if (!status.IsOK()) {
        return indexlib::index::Result<PostingIterator*>(indexlib::index::ErrorCode::Runtime);
    }

    ResultHolder resultHolder(_aithetaIndexConfig.distanceType);
    status = DoSearch(indexQuery, searchInfo, resultHolder, false);
    if (!status.IsOK()) {
        return indexlib::index::Result<PostingIterator*>(indexlib::index::ErrorCode::Runtime);
    }

    size_t topK = 0;
    for (const auto& aithetaQuery : indexQuery.aithetaqueries()) {
        topK += aithetaQuery.topk() * aithetaQuery.embeddingcount();
    }

    const auto& matchItems = resultHolder.GetTopkMatchItems(topK);
    if (nullptr != _recallReporter && !_recallReporter->AsnynReport(indexQuery).IsOK()) {
        AUTIL_LOG(WARN, "async report recall rate failed.");
    }
    METRIC_REPORT(_searchCountMetric, matchItems.size());
    METRIC_REPORT(_filteredCountMetric, resultHolder.GetResultStats().filteredCount);
    METRIC_REPORT(_distCalcCountMetric, resultHolder.GetResultStats().distCalcCount);
    auto annPostingIter = IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, ANNPostingIterator, matchItems, sessionPool);
    return indexlib::index::Result<PostingIterator*>(annPostingIter);
}

Status AithetaIndexReader::InitMetrics(const string& indexName)
{
    if (nullptr == _indexReaderParam.metricsManager ||
        nullptr == _indexReaderParam.metricsManager->GetMetricsReporter()) {
        AUTIL_LOG(WARN, "metric reporter is nullptr");
        return Status::OK();
    }
    auto provider =
        std::make_shared<indexlib::util::MetricProvider>(_indexReaderParam.metricsManager->GetMetricsReporter());
    _metricReporter = std::make_shared<MetricReporter>(provider, indexName);
    METRIC_SETUP(_searchCountMetric, "indexlib.vector.seek_count", kmonitor::GAUGE);
    METRIC_SETUP(_searchLatencyMetric, "indexlib.vector.seek_latency", kmonitor::GAUGE);
    METRIC_SETUP(_searchQpsMetric, "indexlib.vector.seek_qps", kmonitor::QPS);
    METRIC_SETUP(_filteredCountMetric, "indexlib.vector.filtered_count", kmonitor::GAUGE);
    METRIC_SETUP(_distCalcCountMetric, "indexlib.vector.dist_calc_count", kmonitor::GAUGE);

    if (!_aithetaIndexConfig.recallConfig.enable) {
        AUTIL_LOG(WARN, "not enable recall reporter");
        return Status::OK();
    }
    _recallReporter = make_shared<AithetaRecallReporter>(_aithetaIndexConfig, _metricReporter, this);
    return _recallReporter->Init();
}

Status AithetaIndexReader::AddNormalSearcher(std::shared_ptr<IIndexer>& indexer, docid_t segmentBaseDocId,
                                             size_t totalIndexDocCount,
                                             const std::shared_ptr<AithetaFilterCreator>& creator)
{
    if (totalIndexDocCount == 0) {
        AUTIL_LOG(INFO, "no valid index doc count, skip add normal searcher");
        return Status::OK();
    }

    auto diskIndexer = std::dynamic_pointer_cast<AithetaDiskIndexer>(indexer);
    if (nullptr == diskIndexer) {
        RETURN_STATUS_ERROR(InternalError, "cast AithetaDiskIndexer for segment failed");
    }

    auto segmentSearchConfig = _aithetaIndexConfig;
    auto indexSegment = diskIndexer->GetSegment();
    if (indexSegment != nullptr) {
        segmentSearchConfig.searchConfig.scanCount *=
            1.0f * indexSegment->GetSegmentMeta().GetDocCount() / totalIndexDocCount;
        AUTIL_LOG(INFO, "update built segment scan count to [%lu]", segmentSearchConfig.searchConfig.scanCount);
    }

    auto [status, normalSearcher] = diskIndexer->CreateSearcher(segmentSearchConfig, segmentBaseDocId, creator);
    RETURN_IF_STATUS_ERROR(status, "create normal searcher failed.");
    if (nullptr != normalSearcher) {
        _normalSearchers.push_back(normalSearcher);
    }

    return Status::OK();
}

Status AithetaIndexReader::AddRealtimeSearcher(std::shared_ptr<IIndexer>& indexer, docid_t segmentBaseDocId,
                                               const std::shared_ptr<AithetaFilterCreator>& creator)
{
    auto memIndexer = std::dynamic_pointer_cast<AithetaMemIndexer>(indexer);
    if (nullptr == memIndexer) {
        RETURN_STATUS_ERROR(InternalError, "cast AithetaMemIndexer for segment failed");
    }
    auto [status, realtimeSearcher] = memIndexer->CreateSearcher(segmentBaseDocId, creator);
    RETURN_IF_STATUS_ERROR(status, "get realtime searcher for failed");
    if (nullptr != realtimeSearcher) {
        _realtimeSearchers.push_back(realtimeSearcher);
    }
    return Status::OK();
}

Status AithetaIndexReader::ParseQuery(const indexlib::index::Term& term, AithetaQueries& indexQuery,
                                      std::shared_ptr<AithetaAuxSearchInfoBase>& searchInfo)
{
    RETURN_IF_STATUS_ERROR(QueryParser::Parse(_aithetaIndexConfig, _tokenHasher, term.GetWord(), indexQuery),
                           "parse query failed.");
    if (term.GetTermName() == AithetaTerm::AITHETA_TERM_NAME) {
        auto customizedTerm = dynamic_cast<const AithetaTerm*>(&term);
        assert(customizedTerm);
        searchInfo = customizedTerm->GetAithetaSearchInfo();
    }
    return Status::OK();
}

Status AithetaIndexReader::DoSearch(const AithetaQueries& indexQuery,
                                    const std::shared_ptr<AithetaAuxSearchInfoBase>& searchInfo,
                                    ResultHolder& resultHolder, bool searchRtOnly) const
{
    for (auto& searcher : _realtimeSearchers) {
        if (!searcher->Search(indexQuery, searchInfo, resultHolder)) {
            return Status::InternalError("search realtime segment failed");
        }
    }
    if (searchRtOnly) {
        AUTIL_LOG(WARN, "search realtime segment only");
        return Status::OK();
    }
    for (auto& searcher : _normalSearchers) {
        if (!searcher->Search(indexQuery, searchInfo, resultHolder)) {
            return Status::InternalError("search normal segment failed");
        }
    }
    return Status::OK();
}

void AithetaIndexReader::initTokenHasher(const std::shared_ptr<config::ANNIndexConfig>& indexConfig)
{
    auto& fieldConfigVec = indexConfig->GetFieldConfigVector();
    if (fieldConfigVec.size() > 2) {
        auto cateFieldConfig = fieldConfigVec[1];
        if (cateFieldConfig->GetFieldType() == ft_string) {
            _tokenHasher.reset(
                new TokenHasher(cateFieldConfig->GetUserDefinedParam(), cateFieldConfig->GetFieldType()));
        }
    }
}

} // namespace indexlibv2::index::ann
