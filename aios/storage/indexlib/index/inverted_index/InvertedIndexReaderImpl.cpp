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
#include "indexlib/index/inverted_index/InvertedIndexReaderImpl.h"

#include "indexlib/config/TabletSchema.h"
#include "indexlib/index/inverted_index/BufferedPostingIterator.h"
#include "indexlib/index/inverted_index/BuildingIndexReader.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/inverted_index/CompositePostingIterator.h"
#include "indexlib/index/inverted_index/IndexAccessoryReader.h"
#include "indexlib/index/inverted_index/InvertedDiskIndexer.h"
#include "indexlib/index/inverted_index/InvertedIndexMetrics.h"
#include "indexlib/index/inverted_index/InvertedIndexSearchTracer.h"
#include "indexlib/index/inverted_index/InvertedLeafMemReader.h"
#include "indexlib/index/inverted_index/InvertedLeafReader.h"
#include "indexlib/index/inverted_index/InvertedMemIndexer.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapDiskIndexer.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapIndexWriter.h"
#include "indexlib/index/inverted_index/builtin_index/dynamic/DynamicIndexReader.h"
#include "indexlib/index/inverted_index/config/HighFrequencyVocabulary.h"
#include "indexlib/util/FutureExecutor.h"
#include "indexlib/util/PooledUniquePtr.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, InvertedIndexReaderImpl);

namespace {
using document::ModifiedTokens;
using indexlibv2::config::IIndexConfig;
using indexlibv2::config::InvertedIndexConfig;
using indexlibv2::framework::Segment;

bool UseDefaultValue(const std::shared_ptr<IIndexConfig>& indexConfig,
                     std::vector<std::shared_ptr<indexlibv2::config::TabletSchema>> schemas)
{
    if (schemas.size() <= 1) {
        return false;
    }
    for (const auto& schema : schemas) {
        if (schema->GetIndexConfig(indexConfig->GetIndexType(), indexConfig->GetIndexName()) == nullptr) {
            auto invertedIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::InvertedIndexConfig>(indexConfig);
            assert(invertedIndexConfig != nullptr);
            if (!invertedIndexConfig->GetNonTruncateIndexName().empty()) {
                return false;
            }
            return true;
        }
    }
    return false;
}
} // namespace

InvertedIndexReaderImpl::InvertedIndexReaderImpl(const std::shared_ptr<InvertedIndexMetrics>& metrics)
    : _indexMetrics(metrics)
{
}
InvertedIndexReaderImpl::~InvertedIndexReaderImpl() {}

Status InvertedIndexReaderImpl::Open(const std::shared_ptr<IIndexConfig>& indexConfig,
                                     const indexlibv2::framework::TabletData* tabletData)
{
    auto config = std::dynamic_pointer_cast<InvertedIndexConfig>(indexConfig);
    if (config == nullptr) {
        AUTIL_LOG(ERROR, "fail cast to inverted index config");
        return Status::InvalidArgs("fail cast to inverted index config");
    }
    auto segments = tabletData->CreateSlice();
    std::vector<Indexer> indexers;
    auto readSchemaId = tabletData->GetOnDiskVersionReadSchema()->GetSchemaId();
    docid_t baseDocId = 0;

    for (const auto& segment : segments) {
        std::shared_ptr<indexlibv2::index::IIndexer> indexer;
        auto segmentSchemaId = segment->GetSegmentSchema()->GetSchemaId();
        auto docCount = segment->GetSegmentInfo()->GetDocCount();
        if (segment->GetSegmentStatus() == Segment::SegmentStatus::ST_BUILT && docCount == 0) {
            AUTIL_LOG(INFO, "segment [%d] has no doc count, open [%s] do nothing", segment->GetSegmentId(),
                      indexConfig->GetIndexName().c_str());
            continue;
        }

        auto schemas = tabletData->GetAllTabletSchema(std::min(readSchemaId, segmentSchemaId),
                                                      std::max(readSchemaId, segmentSchemaId));
        bool useDefaultValue = UseDefaultValue(indexConfig, schemas);
        if (useDefaultValue) {
            if (readSchemaId < segmentSchemaId) {
                AUTIL_LOG(INFO, "inverted [%s] is deleted in segment [%d]  create no invert reader",
                          indexConfig->GetIndexName().c_str(), segment->GetSegmentId());
                baseDocId += docCount;
                continue;
            }
            auto [status, defaultIndexer] = CreateDefaultDiskIndexer(segment, indexConfig);
            RETURN_IF_STATUS_ERROR(status, "create default indexer failed");
            indexer = defaultIndexer;
            segment->AddIndexer(INVERTED_INDEX_TYPE_STR, config->GetIndexName(), indexer);
            AUTIL_LOG(INFO, "inverted [%s] in segment [%d] use default inverted reader",
                      indexConfig->GetIndexName().c_str(), segment->GetSegmentId());
        } else {
            Status status;
            std::tie(status, indexer) = segment->GetIndexer(indexConfig->GetIndexType(), config->GetIndexName());
            if (!status.IsOK()) {
                RETURN_STATUS_ERROR(InternalError, "no indexer for [%s] in segment [%d]",
                                    indexConfig->GetIndexName().c_str(), segment->GetSegmentId());
            }
        }
        indexers.emplace_back(baseDocId, indexer, segment->GetSegmentId(), segment->GetSegmentStatus());
        baseDocId += docCount;
    }
    auto st = DoOpen(config, indexers);
    RETURN_IF_STATUS_ERROR(st, "do open failed.");

    return TryOpenTruncateIndexReader(config, tabletData);
}

Status InvertedIndexReaderImpl::DoOpen(const std::shared_ptr<InvertedIndexConfig>& indexConfig,
                                       const std::vector<Indexer>& indexers)
{
    _indexConfig = indexConfig;
    _indexFormatOption->Init(_indexConfig);
    _highFreqVol = _indexConfig->GetHighFreqVocabulary();
    _indexSupportNull = _indexConfig->SupportNull();
    _dictHasher = IndexDictHasher(indexConfig->GetDictHashParams(), indexConfig->GetInvertedIndexType());
    std::vector<std::shared_ptr<InvertedLeafReader>> segmentReaders;
    std::vector<docid_t> baseDocIds;
    std::vector<std::pair<docid_t, std::shared_ptr<BitmapLeafReader>>> bitmapDiskReaders;
    std::vector<std::tuple</*baseDocId=*/docid_t, /*segmentDocCount=*/uint64_t,
                           /*dynamicPostingResourceFile=*/std::shared_ptr<file_system::ResourceFile>>>
        dynamicPostingResources;
    std::vector<std::shared_ptr<SegmentPosting>> segmentPostings;
    std::vector<std::pair<docid_t, std::shared_ptr<InMemBitmapIndexSegmentReader>>> bitmapMemReaders;
    std::vector<std::pair<docid_t, std::shared_ptr<DynamicIndexSegmentReader>>> dynamicSegmentReaders;
    for (auto& [baseDocId, indexer, segId, segStatus] : indexers) {
        std::shared_ptr<InvertedLeafReader> reader;
        if (segStatus == Segment::SegmentStatus::ST_BUILT) {
            auto diskIndexer = std::dynamic_pointer_cast<InvertedDiskIndexer>(indexer);
            if (diskIndexer != nullptr) {
                reader = diskIndexer->GetReader();
            }
            if (!reader) {
                // maybe truncate index not exist in segment
                continue;
            }
            if (_highFreqVol != nullptr) {
                bitmapDiskReaders.emplace_back(baseDocId, diskIndexer->GetBitmapDiskIndexer()->GetReader());
            }
            dynamicPostingResources.emplace_back(baseDocId, diskIndexer->GetDocCount(),
                                                 diskIndexer->GetDynamicPostingResource());
            segmentReaders.emplace_back(reader);
            baseDocIds.emplace_back(baseDocId);
        } else {
            std::shared_ptr<IndexSegmentReader> memReader;
            auto memIndexer = std::dynamic_pointer_cast<InvertedMemIndexer>(indexer);
            if (memIndexer != nullptr) {
                memReader = memIndexer->CreateInMemReader();
            }
            if (memReader == nullptr) {
                AUTIL_LOG(ERROR, "failed to create in mem reader for segment[%d]", segId);
                return Status::InternalError("failed to create in mem reader for segment: ", segId);
            }
            if (_highFreqVol != nullptr) {
                if (!memIndexer->GetBitmapIndexWriter()) {
                    AUTIL_LOG(ERROR, "failed to get bitmap index writer, segment[%d],", segId);
                    return Status::InternalError("failed to get bitmap index writer");
                }
                bitmapMemReaders.emplace_back(baseDocId, memIndexer->GetBitmapIndexWriter()->CreateInMemReader());
            }
            std::shared_ptr<DynamicIndexSegmentReader> dynamicSegmentReader = memReader->GetDynamicSegmentReader();
            if (dynamicSegmentReader != nullptr) {
                dynamicSegmentReaders.emplace_back(baseDocId, dynamicSegmentReader);
            }
            AddBuildingSegmentReader(baseDocId, memReader);
            AUTIL_LOG(DEBUG, "Add In-Memory SegmentReader for segment [%d] by index [%s]", segId,
                      _indexConfig->GetIndexName().c_str());
        }
    }
    _segmentReaders.swap(segmentReaders);
    _baseDocIds.swap(baseDocIds);

    if (_highFreqVol != nullptr) {
        _bitmapIndexReader = std::make_unique<BitmapIndexReader>();
        auto status = _bitmapIndexReader->Open(_indexConfig, bitmapDiskReaders, bitmapMemReaders);
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "Open bitmap index [%s] FAILED", _indexConfig->GetIndexName().c_str());
            return status;
        }
    }
    if (_indexConfig->IsIndexUpdatable() && _indexConfig->GetNonTruncateIndexName().empty()) {
        // do not create dynamice index reader if index is truncateIndex
        _dynamicIndexReader = std::make_unique<DynamicIndexReader>();
        _dynamicIndexReader->Open(_indexConfig, dynamicPostingResources, dynamicSegmentReaders);
    }

    // TODO: do not get executor if parallel lookup is off
    _executor = util::FutureExecutor::GetInternalExecutor();
    if (!_executor) {
        AUTIL_LOG(DEBUG, "internal executor not created, use serial mode, index[%s]",
                  _indexConfig->GetIndexName().c_str());
    }
    return Status::OK();
}

Status InvertedIndexReaderImpl::TryOpenTruncateIndexReader(
    const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& invertedIndexConfig,
    const indexlibv2::framework::TabletData* tabletData)
{
    if (!invertedIndexConfig->HasTruncate()) {
        return Status::OK();
    }
    auto truncateTabletData = std::make_shared<indexlibv2::framework::TabletData>("truncate");
    const auto& onDiskVersion = tabletData->GetOnDiskVersion();

    std::vector<std::shared_ptr<Segment>> incSegments;
    auto builtSlice = tabletData->CreateSlice(indexlibv2::framework::Segment::SegmentStatus::ST_BUILT);
    for (const auto& seg : builtSlice) {
        if (onDiskVersion.HasSegment(seg->GetSegmentId())) {
            incSegments.push_back(seg);
        }
    }
    auto st = truncateTabletData->Init(onDiskVersion, incSegments, tabletData->GetResourceMap());
    RETURN_IF_STATUS_ERROR(st, "init truncate tablet data failed.");

    for (const auto& truncateIndexConfig : invertedIndexConfig->GetTruncateIndexConfigs()) {
        // TODO(xc & lc) pass valid metrics
        auto truncateIndexReader = std::make_shared<InvertedIndexReaderImpl>(/*metrics*/ nullptr);
        const auto truncateIndexName = truncateIndexConfig->GetIndexName();
        st = truncateIndexReader->Open(truncateIndexConfig, truncateTabletData.get());
        RETURN_IF_STATUS_ERROR(st, "open truncate index reader failed, index[%s]", truncateIndexName.c_str());
        _truncateIndexReaders[truncateIndexName] = std::move(truncateIndexReader);
    }
    return Status::OK();
}

void InvertedIndexReaderImpl::AddBuildingSegmentReader(docid_t baseDocId,
                                                       const std::shared_ptr<IndexSegmentReader>& segReader)
{
    if (!segReader) {
        return;
    }

    if (!_buildingIndexReader) {
        _buildingIndexReader = CreateBuildingIndexReader();
        assert(_buildingIndexReader);
    }
    _buildingIndexReader->AddSegmentReader(baseDocId, segReader);
}

std::shared_ptr<BuildingIndexReader> InvertedIndexReaderImpl::CreateBuildingIndexReader() const
{
    return std::make_shared<BuildingIndexReader>(_indexFormatOption->GetPostingFormatOption());
}

Result<PostingIterator*> InvertedIndexReaderImpl::Lookup(const Term& term, uint32_t statePoolSize, PostingType type,
                                                         autil::mem_pool::Pool* sessionPool)
{
    return future_lite::coro::syncAwait(DoLookupAsync(&term, {}, statePoolSize, type, sessionPool, nullptr));
}

future_lite::coro::Lazy<Result<PostingIterator*>>
InvertedIndexReaderImpl::DoLookupAsync(const Term* term, const DocIdRangeVector& ranges, uint32_t statePoolSize,
                                       PostingType type, autil::mem_pool::Pool* sessionPool,
                                       file_system::ReadOption option) noexcept
{
    assert(!option.blockCounter); // FIXME:(hanyao) support blockCounter from option

    if (!term) {
        co_return nullptr;
    }

    if (term->IsNull() && !_indexSupportNull) {
        co_return nullptr;
    }

    DocIdRangeVector newRanges;
    if (term->HasHintValues()) {
        newRanges = MergeDocIdRanges(term->GetHintValues(), ranges);
    } else {
        newRanges = ranges;
    }

    if (!ValidatePartitonRange(newRanges)) {
        co_return nullptr;
    }
    bool retNormal = true;
    if (type == pt_default && _highFreqVol != nullptr && _highFreqVol->Lookup(*term)) {
        retNormal = false;
    } else if (type == pt_bitmap && _bitmapIndexReader != nullptr) {
        retNormal = false;
    }

    if (retNormal) {
        if (_dynamicIndexReader) {
            co_return co_await CreateDynamicPostingIteratorAsync(term, newRanges, statePoolSize, sessionPool, option);
        } else {
            co_return co_await CreatePostingIteratorAsync(term, newRanges, statePoolSize, sessionPool, option);
        }
    } else {
        AUTIL_LOG(DEBUG, "index[%s] truncate[%s] word[%s]: bitmap", term->GetIndexName().c_str(),
                  term->GetTruncateIndexName().c_str(), term->GetWord().c_str());
        if (newRanges.empty()) {
            co_return _bitmapIndexReader->Lookup(*term, statePoolSize, sessionPool, option);
        } else {
            co_return _bitmapIndexReader->PartialLookup(*term, newRanges, statePoolSize, sessionPool, option);
        }
    }
}

future_lite::coro::Lazy<Result<PostingIterator*>>
InvertedIndexReaderImpl::CreateDynamicPostingIteratorAsync(const Term* term, const DocIdRangeVector& ranges,
                                                           uint32_t statePoolSize, autil::mem_pool::Pool* sessionPool,
                                                           file_system::ReadOption option) noexcept
{
    Result<DynamicPostingIterator*> dynamicIterResult(nullptr);
    if (ranges.empty()) {
        dynamicIterResult = _dynamicIndexReader->Lookup(*term, statePoolSize, sessionPool);
    } else {
        dynamicIterResult = _dynamicIndexReader->PartialLookup(*term, ranges, statePoolSize, sessionPool);
    }
    if (!dynamicIterResult.Ok()) {
        co_return dynamicIterResult.GetErrorCode();
    }
    DynamicPostingIterator* dynamicIter = dynamicIterResult.Value();

    auto bufferIterResult = co_await CreatePostingIteratorAsync(term, ranges, statePoolSize, sessionPool, option);
    if (!bufferIterResult.Ok()) {
        co_return bufferIterResult.GetErrorCode();
    }
    BufferedPostingIterator* bufferedIter = dynamic_cast<BufferedPostingIterator*>(bufferIterResult.Value());

    if (dynamicIter and bufferedIter) {
        auto iter = IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, CompositePostingIterator<BufferedPostingIterator>,
                                                 sessionPool, bufferedIter, dynamicIter);
        co_return iter;
    } else if (dynamicIter) {
        co_return dynamicIter;
    } else {
        co_return bufferedIter;
    }
}

future_lite::coro::Lazy<Result<PostingIterator*>>
InvertedIndexReaderImpl::CreatePostingIteratorAsync(const Term* term, const DocIdRangeVector& ranges,
                                                    uint32_t statePoolSize, autil::mem_pool::Pool* sessionPool,
                                                    file_system::ReadOption option) noexcept
{
    DictKeyInfo key;
    if (!_dictHasher.GetHashKey(*term, key)) {
        AUTIL_LOG(WARN, "invalid term [%s], index name [%s]", term->GetWord().c_str(), GetIndexName().c_str());
        co_return nullptr;
    }

    bool needTruncPosting = NeedTruncatePosting(*term);
    if (!needTruncPosting && _indexConfig->IsBitmapOnlyTerm(key)) {
        co_return nullptr;
    }

    if (!_executor && !needTruncPosting && ranges.empty() && !_indexFormatOption->HasSectionAttribute()) {
        co_return co_await CreateMainPostingIteratorAsync(key, statePoolSize, sessionPool, true, option);
    }
    co_return co_await CreatePostingIteratorByHashKey(term, key, ranges, statePoolSize, sessionPool, option);
}

future_lite::coro::Lazy<Result<PostingIterator*>> InvertedIndexReaderImpl::CreatePostingIteratorByHashKey(
    const Term* term, DictKeyInfo termHashKey, const DocIdRangeVector& ranges, uint32_t statePoolSize,
    autil::mem_pool::Pool* sessionPool, file_system::ReadOption option) noexcept
{
    auto tracer = util::MakePooledUniquePtr<InvertedIndexSearchTracer>(sessionPool);

    std::vector<future_lite::coro::Lazy<Result<bool>>> tasks;
    SegmentPostingVector segmentPostings;
    bool needBuildingSegment = true;
    if (!ranges.empty()) {
        std::tie(tasks, segmentPostings, needBuildingSegment) =
            FillRangeByBuiltSegments(term, termHashKey, ranges, option, tracer.get());
    } else {
        tasks.reserve(_segmentReaders.size());
        segmentPostings.reserve(_segmentReaders.size() + 1); // for building segments
        for (uint32_t i = 0; i < _segmentReaders.size(); i++) {
            segmentPostings.emplace_back();
            tasks.push_back(
                FillSegmentPostingAsync(term, termHashKey, i, segmentPostings.back(), option, tracer.get()));
        }
    }
    std::vector<future_lite::Try<index::Result<bool>>> results;
    if (_executor) {
        results = co_await future_lite::coro::collectAll(std::move(tasks));
    } else {
        for (size_t i = 0; i < tasks.size(); ++i) {
            results.emplace_back(co_await tasks[i]);
        }
    }

    assert(results.size() == segmentPostings.size());
    std::shared_ptr<SegmentPostingVector> resultSegPostings(new SegmentPostingVector);
    size_t reserveCount = segmentPostings.size();
    size_t buildingSegCount = 0;
    if (_buildingIndexReader) {
        buildingSegCount = _buildingIndexReader->GetSegmentCount();
        reserveCount += buildingSegCount;
    }
    tracer->SetSearchedSegmentCount(reserveCount);
    tracer->SetSearchedInMemSegmentCount(buildingSegCount);
    resultSegPostings->reserve(reserveCount);
    for (size_t i = 0; i < results.size(); ++i) {
        assert(!results[i].hasError());
        if (!results[i].value().Ok()) {
            co_return results[i].value().GetErrorCode();
        }
        bool fillSuccess = results[i].value().Value();
        if (fillSuccess) {
            resultSegPostings->push_back(std::move(segmentPostings[i]));
        }
    }

    if (_buildingIndexReader && needBuildingSegment) {
        _buildingIndexReader->GetSegmentPosting(termHashKey, *resultSegPostings, sessionPool, tracer.get());
    }

    if (resultSegPostings->size() == 0) {
        co_return nullptr;
    }

    SectionAttributeReader* pSectionReader = nullptr;
    if (_accessoryReader) {
        pSectionReader = _accessoryReader->GetSectionReader(_indexConfig->GetIndexName()).get();
    }

    std::unique_ptr<BufferedPostingIterator, std::function<void(BufferedPostingIterator*)>> iter(
        CreateBufferedPostingIterator(sessionPool, std::move(tracer)),
        [sessionPool](BufferedPostingIterator* iter) { IE_POOL_COMPATIBLE_DELETE_CLASS(sessionPool, iter); });

    if (iter->Init(resultSegPostings, pSectionReader, statePoolSize)) {
        co_return iter.release();
    }
    co_return nullptr;
}

BufferedPostingIterator*
InvertedIndexReaderImpl::CreateBufferedPostingIterator(autil::mem_pool::Pool* sessionPool,
                                                       util::PooledUniquePtr<InvertedIndexSearchTracer> tracer) const
{
    return IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, BufferedPostingIterator,
                                        _indexFormatOption->GetPostingFormatOption(), sessionPool, std::move(tracer));
}

void InvertedIndexReaderImpl::SetAccessoryReader(const std::shared_ptr<IndexAccessoryReader>& accessoryReader)
{
    InvertedIndexType indexType = _indexConfig->GetInvertedIndexType();
    if (indexType == it_pack || indexType == it_expack) {
        _accessoryReader = accessoryReader;
    } else {
        _accessoryReader.reset();
    }
}

bool InvertedIndexReaderImpl::NeedTruncatePosting(const Term& term) const
{
    if (!_truncateIndexReaders.empty() && !term.GetIndexName().empty() && !term.GetTruncateName().empty()) {
        return true;
    }

    const LiteTerm* liteTerm = term.GetLiteTerm();
    if (liteTerm) {
        return !_truncateIndexReaders.empty() && liteTerm->GetTruncateIndexId() != INVALID_INDEXID;
    }
    return false;
}

bool InvertedIndexReaderImpl::ValidatePartitonRange(const DocIdRangeVector& ranges)
{
    if (ranges.empty()) {
        return true;
    }
    docid_t lastEndDocId = INVALID_DOCID;
    for (const auto& range : ranges) {
        if (range.first >= range.second) {
            AUTIL_LOG(ERROR, "range [%d, %d) is invalid", range.first, range.second);
            return false;
        }
        if (range.first < lastEndDocId) {
            AUTIL_LOG(ERROR, "range [%d, %d) is invalid, previous range ends at [%d]", range.first, range.second,
                      lastEndDocId);
            return false;
        }
        lastEndDocId = range.second;
    }
    return true;
}

DocIdRangeVector InvertedIndexReaderImpl::MergeDocIdRanges(int32_t hintValues, const DocIdRangeVector& ranges) const
{
    return ranges;
}

future_lite::coro::Lazy<Result<PostingIterator*>>
InvertedIndexReaderImpl::LookupAsync(const Term* term, uint32_t statePoolSize, PostingType type,
                                     autil::mem_pool::Pool* pool, file_system::ReadOption option) noexcept
{
    co_return co_await DoLookupAsync(term, {}, statePoolSize, type, pool, option);
}

Result<PostingIterator*> InvertedIndexReaderImpl::PartialLookup(const Term& term, const DocIdRangeVector& ranges,
                                                                uint32_t statePoolSize, PostingType type,
                                                                autil::mem_pool::Pool* sessionPool)
{
    return future_lite::coro::syncAwait(DoLookupAsync(&term, ranges, statePoolSize, type, sessionPool, nullptr));
}

const SectionAttributeReader* InvertedIndexReaderImpl::GetSectionReader(const std::string& indexName) const
{
    if (_accessoryReader) {
        return _accessoryReader->GetSectionReader(indexName).get();
    }
    return nullptr;
}

std::vector<std::shared_ptr<DictionaryReader>> InvertedIndexReaderImpl::GetDictReaders() const
{
    std::vector<std::shared_ptr<DictionaryReader>> dictReaders;
    for (size_t i = 0; i < _segmentReaders.size(); ++i) {
        dictReaders.push_back(_segmentReaders[i]->GetDictionaryReader());
    }
    if (_buildingIndexReader) {
        std::vector<std::shared_ptr<DictionaryReader>> buildingDictReaders =
            _buildingIndexReader->GetDictionaryReader();
        dictReaders.insert(dictReaders.end(), buildingDictReaders.begin(), buildingDictReaders.end());
    }
    return dictReaders;
}

future_lite::coro::Lazy<Result<PostingIterator*>>
InvertedIndexReaderImpl::CreateMainPostingIteratorAsync(DictKeyInfo key, uint32_t statePoolSize,
                                                        autil::mem_pool::Pool* sessionPool, bool needBuildingSegment,
                                                        file_system::ReadOption option) noexcept
{
    auto tracer = util::MakePooledUniquePtr<InvertedIndexSearchTracer>(sessionPool);

    std::shared_ptr<SegmentPostingVector> segPostings(new SegmentPostingVector);
    size_t count = _segmentReaders.size();
    size_t buildingCount = 0;
    if (needBuildingSegment && _buildingIndexReader) {
        buildingCount = _buildingIndexReader->GetSegmentCount();
        count += buildingCount;
    }

    segPostings->reserve(count);
    tracer->SetSearchedSegmentCount(count);
    tracer->SetSearchedInMemSegmentCount(buildingCount);

    for (uint32_t i = 0; i < _segmentReaders.size(); i++) {
        SegmentPosting segPosting;
        auto ret = co_await GetSegmentPostingAsync(key, i, segPosting, option, tracer.get());
        if (ret.Ok()) {
            if (ret.Value()) {
                segPostings->push_back(std::move(segPosting));
            }
        } else {
            co_return ret.GetErrorCode();
        }
    }

    if (needBuildingSegment && _buildingIndexReader) {
        _buildingIndexReader->GetSegmentPosting(key, *segPostings, sessionPool, tracer.get());
    }

    BufferedPostingIterator* iter =
        IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, BufferedPostingIterator, _indexFormatOption->GetPostingFormatOption(),
                                     sessionPool, std::move(tracer));
    if (iter->Init(segPostings, nullptr, statePoolSize)) {
        co_return iter;
    }
    IE_POOL_COMPATIBLE_DELETE_CLASS(sessionPool, iter);
    co_return nullptr;
}

std::tuple<std::vector<future_lite::coro::Lazy<index::Result<bool>>>, SegmentPostingVector, bool>
InvertedIndexReaderImpl::FillRangeByBuiltSegments(const Term* term, const DictKeyInfo& termHashKey,
                                                  const DocIdRangeVector& ranges, file_system::ReadOption option,
                                                  InvertedIndexSearchTracer* tracer) noexcept
{
    std::vector<future_lite::coro::Lazy<index::Result<bool>>> tasks;
    tasks.reserve(_segmentReaders.size());
    SegmentPostingVector segmentPostings;
    segmentPostings.reserve(_segmentReaders.size() + 1); // for building segments
    bool needBuildingSegment = true;

    size_t currentRangeIdx = 0;
    size_t currentSegmentIdx = 0;
    bool currentSegmentFilled = false;
    while (currentSegmentIdx < _segmentReaders.size() && currentRangeIdx < ranges.size()) {
        const auto& range = ranges[currentRangeIdx];
        docid_t segBegin = _baseDocIds[currentSegmentIdx];
        docid_t segEnd = _baseDocIds[currentSegmentIdx] + _segmentReaders[currentSegmentIdx]->GetDocCount();
        if (segEnd <= range.first) {
            ++currentSegmentIdx;
            currentSegmentFilled = false;
            continue;
        }
        if (segBegin >= range.second) {
            ++currentRangeIdx;
            continue;
        }
        if (!currentSegmentFilled) {
            segmentPostings.emplace_back();
            tasks.push_back(
                FillSegmentPostingAsync(term, termHashKey, currentSegmentIdx, segmentPostings.back(), option, tracer));
            currentSegmentFilled = true;
        }

        auto minEnd = std::min(segEnd, range.second);
        if (segEnd == minEnd) {
            ++currentSegmentIdx;
            currentSegmentFilled = false;
        }
        if (range.second == minEnd) {
            ++currentRangeIdx;
        }
    }
    needBuildingSegment = (currentRangeIdx < ranges.size());
    return {std::move(tasks), std::move(segmentPostings), needBuildingSegment};
}

future_lite::coro::Lazy<Result<bool>>
InvertedIndexReaderImpl::FillSegmentPostingAsync(const Term* term, const DictKeyInfo& key, uint32_t segmentIdx,
                                                 SegmentPosting& segPosting, file_system::ReadOption option,
                                                 InvertedIndexSearchTracer* tracer) noexcept
{
    if (NeedTruncatePosting(*term)) {
        co_return co_await FillTruncSegmentPosting(*term, key, segmentIdx, segPosting, option, tracer);
    }
    co_return co_await GetSegmentPostingAsync(key, segmentIdx, segPosting, option, tracer);
}

future_lite::coro::Lazy<Result<bool>>
InvertedIndexReaderImpl::FillTruncSegmentPosting(const Term& term, const DictKeyInfo& key, uint32_t segmentIdx,
                                                 SegmentPosting& segPosting, file_system::ReadOption option,
                                                 InvertedIndexSearchTracer* tracer) noexcept
{
    SegmentPosting truncSegPosting;
    auto truncateResult =
        co_await GetSegmentPostingFromTruncIndex(term, key, segmentIdx, option, truncSegPosting, tracer);
    if (!truncateResult.Ok()) {
        co_return truncateResult.GetErrorCode();
    }
    bool hasTruncateSeg = truncateResult.Value();
    SegmentPosting mainChainSegPosting;
    auto mainResult = co_await GetSegmentPostingAsync(key, segmentIdx, mainChainSegPosting, option, tracer);
    if (!mainResult.Ok()) {
        co_return mainResult.GetErrorCode();
    }
    bool hasMainChainSeg = mainResult.Value();

    if (!hasTruncateSeg) {
        if (hasMainChainSeg) {
            segPosting = mainChainSegPosting;
            co_return true;
        } else {
            co_return false;
        }
    } else {
        if (tracer) {
            tracer->SetTruncateTerm();
        }
        segPosting = truncSegPosting;
    }

    TermMeta termMeta;
    auto metaRet = GetMainChainTermMeta(mainChainSegPosting, key, segmentIdx, termMeta, option, tracer);
    if (!metaRet.Ok()) {
        co_return metaRet.GetErrorCode();
    }
    if (metaRet.Value()) {
        segPosting.SetMainChainTermMeta(termMeta);
    }
    co_return metaRet.Value();
}

Result<bool> InvertedIndexReaderImpl::GetMainChainTermMeta(const SegmentPosting& mainChainSegPosting,
                                                           const DictKeyInfo& key, uint32_t segmentIdx,
                                                           TermMeta& termMeta, file_system::ReadOption option,
                                                           InvertedIndexSearchTracer* tracer) noexcept
{
    if (mainChainSegPosting.GetSingleSlice() || mainChainSegPosting.GetSliceListPtr() ||
        mainChainSegPosting.IsDictInline()) {
        termMeta = mainChainSegPosting.GetMainChainTermMeta();
        return true;
    }

    // TODO: get bitmap chain, to get original df
    if (!_bitmapIndexReader) {
        AUTIL_LOG(ERROR, "Has truncate chain but no main chain, should has bitmap chain");
        return false;
    }

    const PostingFormatOption& postingFormatOption = mainChainSegPosting.GetPostingFormatOption();

    SegmentPosting bitmapSegPosting(postingFormatOption.GetBitmapPostingFormatOption());

    auto hasBitmapChainSegRet =
        _bitmapIndexReader->GetSegmentPosting(key, segmentIdx, bitmapSegPosting, option, tracer);
    if (!hasBitmapChainSegRet.Ok()) {
        return hasBitmapChainSegRet.GetErrorCode();
    }
    if (!hasBitmapChainSegRet.Value()) {
        AUTIL_LOG(ERROR, "Has truncate chain but no main chain, should has bitmap chain");
        return false;
    }

    termMeta = bitmapSegPosting.GetMainChainTermMeta();
    return true;
}

std::pair<Status, std::shared_ptr<InvertedDiskIndexer>>
InvertedIndexReaderImpl::CreateDefaultDiskIndexer(const std::shared_ptr<Segment>& segment,
                                                  const std::shared_ptr<IIndexConfig>& indexConfig)
{
    indexlibv2::index::IndexerParameter indexerParam;
    indexerParam.segmentId = segment->GetSegmentId();
    indexerParam.docCount = segment->GetSegmentInfo()->docCount;
    indexerParam.segmentInfo = segment->GetSegmentInfo();
    indexerParam.segmentMetrics = segment->GetSegmentMetrics();
    indexerParam.readerOpenType = indexlibv2::index::IndexerParameter::READER_DEFAULT_VALUE;

    auto [status, directory] =
        segment->GetSegmentDirectory()->GetIDirectory()->GetDirectory(INVERTED_INDEX_PATH).StatusWith();
    RETURN2_IF_STATUS_ERROR(status, nullptr, "get invert index path failed, segment [%d]", segment->GetSegmentId());

    std::shared_ptr<InvertedDiskIndexer> diskIndexer(new InvertedDiskIndexer(indexerParam));
    auto status2 = diskIndexer->Open(indexConfig, directory);
    RETURN2_IF_STATUS_ERROR(status2, nullptr, "invert [%s] in segment [%d] open default indexer failed",
                            indexConfig->GetIndexName().c_str(), segment->GetSegmentId());
    return {Status::OK(), diskIndexer};
}

future_lite::coro::Lazy<Result<bool>>
InvertedIndexReaderImpl::GetSegmentPostingFromTruncIndex(const Term& term, const DictKeyInfo& key, uint32_t segmentIdx,
                                                         file_system::ReadOption option, SegmentPosting& segPosting,
                                                         InvertedIndexSearchTracer* tracer) noexcept
{
    const LiteTerm* liteTerm = term.GetLiteTerm();
    std::shared_ptr<InvertedIndexReader> truncateIndexReader;
    if (term.HasTruncateName()) {
        auto it = _truncateIndexReaders.find(GetIndexName() + "_" + term.GetTruncateName());
        if (it != _truncateIndexReaders.end()) {
            truncateIndexReader = it->second;
        }
    } else if (liteTerm && liteTerm->GetTruncateIndexId() != INVALID_INDEXID) {
        AUTIL_LOG(ERROR, "un-support query format, index[%s] truncate[%s] indexid[%u]", GetIndexName().c_str(),
                  term.GetTruncateName().c_str(), liteTerm->GetTruncateIndexId());
        co_return false;
    }

    if (!truncateIndexReader) {
        co_return false;
    }
    co_return co_await truncateIndexReader->GetSegmentPostingAsync(key, segmentIdx, segPosting, option, tracer);
}

} // namespace indexlib::index
