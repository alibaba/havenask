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
#include "indexlib/index/normal/inverted_index/accessor/normal_index_reader.h"

#include "future_lite/MoveWrapper.h"
#include "indexlib/index/common/KeyHasherWrapper.h"
#include "indexlib/index/inverted_index/BufferedPostingIterator.h"
#include "indexlib/index/inverted_index/BuildingIndexReader.h"
#include "indexlib/index/inverted_index/CompositePostingIterator.h"
#include "indexlib/index/inverted_index/IndexSegmentReader.h"
#include "indexlib/index/inverted_index/SegmentPosting.h"
#include "indexlib/index/inverted_index/builtin_index/dynamic/DynamicPostingIterator.h"
#include "indexlib/index/inverted_index/config/HighFrequencyVocabulary.h"
#include "indexlib/index/inverted_index/patch/IndexUpdateTermIterator.h"
#include "indexlib/index/normal/attribute/accessor/section_attribute_reader_impl.h"
#include "indexlib/index/normal/framework/multi_field_index_segment_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/index_accessory_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_field_index_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_segment_reader.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_index_reader.h"
#include "indexlib/index/normal/inverted_index/builtin_index/dynamic/dynamic_index_reader.h"
#include "indexlib/index_base/index_meta/temperature_doc_info.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/in_memory_segment_reader.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/FutureExecutor.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"

using namespace std;
using namespace autil;

using future_lite::Try;
using future_lite::Unit;
using future_lite::coro::Lazy;

using namespace indexlib::index;
using namespace indexlib::config;
using namespace indexlib::index_base;
using namespace indexlib::common;
using namespace indexlib::util;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, NormalIndexReader);

NormalIndexReader::NormalIndexReader() : NormalIndexReader(nullptr) {}

NormalIndexReader::NormalIndexReader(IndexMetrics* indexMetrics)
    : mBitmapIndexReader(NULL)
    , mDynamicIndexReader(NULL)
    , mMultiFieldIndexReader(NULL)
    , mExecutor(nullptr)
    , mIndexSupportNull(false)
    , mIndexMetrics(indexMetrics)
    , mTemperatureDocInfo(nullptr)
{
}

NormalIndexReader::~NormalIndexReader()
{
    DELETE_AND_SET_NULL(mBitmapIndexReader);
    DELETE_AND_SET_NULL(mDynamicIndexReader);
}

index::Result<PostingIterator*> NormalIndexReader::Lookup(const Term& term, uint32_t statePoolSize, PostingType type,
                                                          autil::mem_pool::Pool* sessionPool)
{
    return future_lite::coro::syncAwait(DoLookupAsync(&term, {}, statePoolSize, type, sessionPool, /*option*/ nullptr));
}

Lazy<index::Result<PostingIterator*>> NormalIndexReader::LookupAsync(const Term* term, uint32_t statePoolSize,
                                                                     PostingType type,
                                                                     autil::mem_pool::Pool* sessionPool,
                                                                     file_system::ReadOption option) noexcept
{
    co_return co_await DoLookupAsync(term, {}, statePoolSize, type, sessionPool, option);
}

Lazy<index::Result<PostingIterator*>> NormalIndexReader::DoLookupAsync(const Term* term, const DocIdRangeVector& ranges,
                                                                       uint32_t statePoolSize, PostingType type,
                                                                       autil::mem_pool::Pool* sessionPool,
                                                                       file_system::ReadOption option) noexcept
{
    if (!term) {
        co_return nullptr;
    }

    if (term->IsNull() && !mIndexSupportNull) {
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
    if (type == pt_default && mHighFreqVol && mHighFreqVol->Lookup(*term)) {
        retNormal = false;
    } else if (type == pt_bitmap && mBitmapIndexReader != NULL) {
        retNormal = false;
    }

    if (retNormal) {
        DynamicPostingIterator* dynamicIter = nullptr;
        BufferedPostingIterator* bufferedIter = nullptr;
        if (mDynamicIndexReader) {
            if (newRanges.empty()) {
                auto iterRet = mDynamicIndexReader->Lookup(*term, statePoolSize, sessionPool);
                if (iterRet.Ok()) {
                    dynamicIter = iterRet.Value();
                } else {
                    co_return iterRet.GetErrorCode();
                }

            } else {
                auto iterRet = mDynamicIndexReader->Lookup(*term, statePoolSize, sessionPool);
                if (iterRet.Ok()) {
                    dynamicIter = iterRet.Value();
                } else {
                    co_return iterRet.GetErrorCode();
                }
            }
            auto bufferIterResult =
                co_await CreatePostingIteratorAsync(term, newRanges, statePoolSize, sessionPool, option);
            if (!bufferIterResult.Ok()) {
                co_return bufferIterResult.GetErrorCode();
            }
            bufferedIter = dynamic_cast<BufferedPostingIterator*>(bufferIterResult.Value());
        } else {
            co_return co_await CreatePostingIteratorAsync(term, newRanges, statePoolSize, sessionPool, option);
        }
        if (dynamicIter and bufferedIter) {
            auto iter = IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, CompositePostingIterator<BufferedPostingIterator>,
                                                     sessionPool, bufferedIter, dynamicIter);
            co_return iter;
        } else if (dynamicIter) {
            co_return dynamicIter;
        } else {
            co_return bufferedIter;
        }
    } else {
        IE_LOG(DEBUG, "index[%s] truncate[%s] word[%s]: bitmap", term->GetIndexName().c_str(),
               term->GetTruncateIndexName().c_str(), term->GetWord().c_str());
        if (newRanges.empty()) {
            co_return mBitmapIndexReader->Lookup(*term, statePoolSize, sessionPool, option);
        } else {
            co_return mBitmapIndexReader->PartialLookup(*term, newRanges, statePoolSize, sessionPool, option);
        }
    }
}

bool NormalIndexReader::ValidatePartitonRange(const DocIdRangeVector& ranges)
{
    if (ranges.empty()) {
        return true;
    }
    docid_t lastEndDocId = INVALID_DOCID;
    for (const auto& range : ranges) {
        if (range.first >= range.second) {
            IE_LOG(ERROR, "range [%d, %d) is invalid", range.first, range.second);
            return false;
        }
        if (range.first < lastEndDocId) {
            IE_LOG(ERROR, "range [%d, %d) is invalid, previous range ends at [%d]", range.first, range.second,
                   lastEndDocId);
            return false;
        }
        lastEndDocId = range.second;
    }
    return true;
}

index::Result<PostingIterator*> NormalIndexReader::PartialLookup(const Term& term, const DocIdRangeVector& ranges,
                                                                 uint32_t statePoolSize, PostingType type,
                                                                 autil::mem_pool::Pool* sessionPool)
{
    return future_lite::coro::syncAwait(
        DoLookupAsync(&term, ranges, statePoolSize, type, sessionPool, /*option*/ nullptr));
}

future_lite::coro::Lazy<index::Result<bool>>
NormalIndexReader::FillTruncSegmentPosting(const Term& term, const index::DictKeyInfo& key, uint32_t segmentIdx,
                                           SegmentPosting& segPosting, file_system::ReadOption option) noexcept
{
    SegmentPosting truncSegPosting;
    auto truncateResult = co_await GetSegmentPostingFromTruncIndex(term, key, segmentIdx, option, truncSegPosting);
    if (!truncateResult.Ok()) {
        co_return truncateResult.GetErrorCode();
    }
    bool hasTruncateSeg = truncateResult.Value();

    SegmentPosting mainChainSegPosting;
    auto mainResult = co_await GetSegmentPostingAsync(key, segmentIdx, mainChainSegPosting, option);
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
        segPosting = truncSegPosting;
    }

    TermMeta termMeta;
    auto metaRet = GetMainChainTermMeta(mainChainSegPosting, key, segmentIdx, termMeta, option);
    if (!metaRet.Ok()) {
        co_return metaRet.GetErrorCode();
    }
    if (metaRet.Value()) {
        segPosting.SetMainChainTermMeta(termMeta);
    }
    co_return metaRet.Value();
}

future_lite::coro::Lazy<index::Result<bool>>
NormalIndexReader::GetSegmentPostingFromTruncIndex(const Term& term, const index::DictKeyInfo& key, uint32_t segmentIdx,
                                                   file_system::ReadOption option, SegmentPosting& segPosting) noexcept
{
    const LiteTerm* liteTerm = term.GetLiteTerm();
    std::shared_ptr<InvertedIndexReader> truncateIndexReader;
    if (term.HasTruncateName()) {
        truncateIndexReader = mMultiFieldIndexReader->GetInvertedIndexReader(term.GetTruncateIndexName());
    } else if (liteTerm && liteTerm->GetTruncateIndexId() != INVALID_INDEXID) {
        truncateIndexReader = mMultiFieldIndexReader->GetIndexReaderWithIndexId(liteTerm->GetTruncateIndexId());
    }

    if (!truncateIndexReader) {
        co_return false;
    }
    co_return co_await truncateIndexReader->GetSegmentPostingAsync(key, segmentIdx, segPosting, option,
                                                                   /*tracer*/ nullptr);
}

index::Result<bool> NormalIndexReader::GetMainChainTermMeta(const SegmentPosting& mainChainSegPosting,
                                                            const index::DictKeyInfo& key, uint32_t segmentIdx,
                                                            TermMeta& termMeta, file_system::ReadOption option) noexcept
{
    if (mainChainSegPosting.GetSingleSlice() || mainChainSegPosting.GetSliceListPtr() ||
        mainChainSegPosting.IsDictInline()) {
        termMeta = mainChainSegPosting.GetMainChainTermMeta();
        return true;
    }

    // TODO: get bitmap chain, to get original df
    if (!mBitmapIndexReader) {
        IE_LOG(ERROR, "Has truncate chain but no main chain, should has bitmap chain");
        return false;
    }

    const PostingFormatOption& postingFormatOption = mainChainSegPosting.GetPostingFormatOption();

    SegmentPosting bitmapSegPosting(postingFormatOption.GetBitmapPostingFormatOption());

    auto hasBitmapChainSegRet = mBitmapIndexReader->GetSegmentPosting(key, segmentIdx, bitmapSegPosting, option);
    if (!hasBitmapChainSegRet.Ok()) {
        return hasBitmapChainSegRet.GetErrorCode();
    }
    if (!hasBitmapChainSegRet.Value()) {
        IE_LOG(ERROR, "Has truncate chain but no main chain, should has bitmap chain");
        return false;
    }

    termMeta = bitmapSegPosting.GetMainChainTermMeta();
    return true;
}

Lazy<index::Result<bool>> NormalIndexReader::FillSegmentPostingAsync(const Term* term, const index::DictKeyInfo& key,
                                                                     uint32_t segmentIdx, SegmentPosting& segPosting,
                                                                     file_system::ReadOption option) noexcept
{
    if (NeedTruncatePosting(*term)) {
        co_return co_await FillTruncSegmentPosting(*term, key, segmentIdx, segPosting, option);
    }
    co_return co_await GetSegmentPostingAsync(key, segmentIdx, segPosting, option);
}

Lazy<index::Result<index::PostingIterator*>> NormalIndexReader::CreatePostingIteratorByHashKey(
    const Term* term, index::DictKeyInfo termHashKey, const DocIdRangeVector& ranges, uint32_t statePoolSize,
    autil::mem_pool::Pool* sessionPool, file_system::ReadOption option) noexcept
{
    vector<Lazy<index::Result<bool>>> tasks;
    tasks.reserve(mSegmentReaders.size());
    SegmentPostingVector segmentPostings;
    segmentPostings.reserve(mSegmentReaders.size() + 1); // for building segments
    bool needBuildingSegment = true;
    if (!ranges.empty()) {
        FillRangeByBuiltSegments(term, termHashKey, ranges, tasks, segmentPostings, needBuildingSegment, option);
    } else {
        for (uint32_t i = 0; i < mSegmentReaders.size(); i++) {
            segmentPostings.emplace_back();
            tasks.push_back(FillSegmentPostingAsync(term, termHashKey, i, segmentPostings.back(), option));
        }
    }
    std::vector<future_lite::Try<index::Result<bool>>> results;
    if (mExecutor) {
        results = co_await future_lite::coro::collectAll(std::move(tasks));
    } else {
        for (size_t i = 0; i < tasks.size(); ++i) {
            results.emplace_back(co_await tasks[i]);
        }
    }

    assert(results.size() == segmentPostings.size());
    shared_ptr<SegmentPostingVector> resultSegPostings(new SegmentPostingVector);
    size_t reserveCount = segmentPostings.size();
    if (mBuildingIndexReader) {
        reserveCount += mBuildingIndexReader->GetSegmentCount();
    }
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

    if (mBuildingIndexReader && needBuildingSegment) {
        mBuildingIndexReader->GetSegmentPosting(termHashKey, *resultSegPostings, sessionPool);
    }

    if (resultSegPostings->size() == 0) {
        co_return nullptr;
    }

    LegacySectionAttributeReaderImpl* pSectionReader = NULL;
    if (mAccessoryReader) {
        pSectionReader = mAccessoryReader->GetSectionReader(_indexConfig->GetIndexName()).get();
    }

    std::unique_ptr<BufferedPostingIterator, std::function<void(BufferedPostingIterator*)>> iter(
        CreateBufferedPostingIterator(sessionPool, /*tracer*/ nullptr),
        [sessionPool](BufferedPostingIterator* iter) { IE_POOL_COMPATIBLE_DELETE_CLASS(sessionPool, iter); });

    if (iter->Init(resultSegPostings, pSectionReader, statePoolSize)) {
        co_return iter.release();
    }
    co_return nullptr;
}

void NormalIndexReader::FillRangeByBuiltSegments(const index::Term* term, const index::DictKeyInfo& termHashKey,
                                                 const DocIdRangeVector& ranges,
                                                 vector<Lazy<index::Result<bool>>>& tasks,
                                                 SegmentPostingVector& segPostings, bool& needBuildingSegment,
                                                 file_system::ReadOption option) noexcept
{
    size_t currentRangeIdx = 0;
    size_t currentSegmentIdx = 0;
    bool currentSegmentFilled = false;
    while (currentSegmentIdx < mSegmentReaders.size() && currentRangeIdx < ranges.size()) {
        const auto& range = ranges[currentRangeIdx];
        const auto& segData = mSegmentReaders[currentSegmentIdx]->GetSegmentData();
        docid_t segBegin = segData.GetBaseDocId();
        docid_t segEnd = segData.GetBaseDocId() + segData.GetSegmentInfo()->docCount;
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
            segPostings.emplace_back();
            tasks.push_back(FillSegmentPostingAsync(term, termHashKey, currentSegmentIdx, segPostings.back(), option));
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
}

future_lite::coro::Lazy<index::Result<PostingIterator*>>
NormalIndexReader::CreatePostingIteratorAsync(const Term* term, const DocIdRangeVector& ranges, uint32_t statePoolSize,
                                              autil::mem_pool::Pool* sessionPool,
                                              file_system::ReadOption option) noexcept
{
    index::DictKeyInfo key;
    if (!mDictHasher.GetHashKey(*term, key)) {
        IE_LOG(WARN, "invalid term [%s], index name [%s]", term->GetWord().c_str(), GetIndexName().c_str());
        co_return nullptr;
    }

    bool needTruncPosting = NeedTruncatePosting(*term);
    if (!needTruncPosting && _indexConfig->IsBitmapOnlyTerm(key)) {
        co_return nullptr;
    }

    if (!mExecutor && !needTruncPosting && ranges.empty() && !_indexFormatOption->HasSectionAttribute()) {
        co_return co_await CreateMainPostingIteratorAsync(key, statePoolSize, sessionPool, true, option);
    }
    co_return co_await CreatePostingIteratorByHashKey(term, key, ranges, statePoolSize, sessionPool, option);
}

BufferedPostingIterator*
NormalIndexReader::CreateBufferedPostingIterator(autil::mem_pool::Pool* sessionPool,
                                                 util::PooledUniquePtr<InvertedIndexSearchTracer> tracer) const
{
    return IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, BufferedPostingIterator,
                                        _indexFormatOption->GetPostingFormatOption(), sessionPool, std::move(tracer));
}

PostingIterator* NormalIndexReader::CreateMainPostingIterator(index::DictKeyInfo key, uint32_t statePoolSize,
                                                              autil::mem_pool::Pool* sessionPool,
                                                              bool needBuildingSegment)
{
    shared_ptr<SegmentPostingVector> segPostings(new SegmentPostingVector);
    size_t count = mSegmentReaders.size();
    if (needBuildingSegment && mBuildingIndexReader) {
        count += mBuildingIndexReader->GetSegmentCount();
    }

    segPostings->reserve(count);
    for (uint32_t i = 0; i < mSegmentReaders.size(); i++) {
        SegmentPosting segPosting;
        if (GetSegmentPosting(key, i, segPosting)) {
            segPostings->push_back(std::move(segPosting));
        }
    }

    if (needBuildingSegment && mBuildingIndexReader) {
        mBuildingIndexReader->GetSegmentPosting(key, *segPostings, sessionPool);
    }

    BufferedPostingIterator* iter =
        IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, BufferedPostingIterator, _indexFormatOption->GetPostingFormatOption(),
                                     sessionPool, /*tracer*/ nullptr);
    if (iter->Init(segPostings, NULL, statePoolSize)) {
        return iter;
    }
    IE_POOL_COMPATIBLE_DELETE_CLASS(sessionPool, iter);
    return NULL;
}

future_lite::coro::Lazy<index::Result<PostingIterator*>> NormalIndexReader::CreateMainPostingIteratorAsync(
    index::DictKeyInfo key, uint32_t statePoolSize, autil::mem_pool::Pool* sessionPool, bool needBuildingSegment,
    file_system::ReadOption option, indexlib::index::InvertedIndexSearchTracer* tracer) noexcept
{
    shared_ptr<SegmentPostingVector> segPostings(new SegmentPostingVector);
    size_t count = mSegmentReaders.size();
    if (needBuildingSegment && mBuildingIndexReader) {
        count += mBuildingIndexReader->GetSegmentCount();
    }

    segPostings->reserve(count);
    for (uint32_t i = 0; i < mSegmentReaders.size(); i++) {
        SegmentPosting segPosting;
        auto ret = co_await GetSegmentPostingAsync(key, i, segPosting, option);
        if (ret.Ok()) {
            if (ret.Value()) {
                segPostings->push_back(std::move(segPosting));
            }
        } else {
            co_return ret.GetErrorCode();
        }
    }

    if (needBuildingSegment && mBuildingIndexReader) {
        mBuildingIndexReader->GetSegmentPosting(key, *segPostings, sessionPool);
    }

    BufferedPostingIterator* iter =
        IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, BufferedPostingIterator, _indexFormatOption->GetPostingFormatOption(),
                                     sessionPool, /*tracer*/ nullptr);
    if (iter->Init(segPostings, NULL, statePoolSize)) {
        co_return iter;
    }
    IE_POOL_COMPATIBLE_DELETE_CLASS(sessionPool, iter);
    co_return nullptr;
}

bool NormalIndexReader::NeedTruncatePosting(const Term& term) const
{
    if (mMultiFieldIndexReader && !term.GetIndexName().empty() && !term.GetTruncateName().empty()) {
        return true;
    }

    const LiteTerm* liteTerm = term.GetLiteTerm();
    if (liteTerm) {
        return mMultiFieldIndexReader && liteTerm->GetTruncateIndexId() != INVALID_INDEXID;
    }
    return false;
}

const SectionAttributeReader* NormalIndexReader::GetSectionReader(const string& indexName) const
{
    if (mAccessoryReader) {
        return mAccessoryReader->GetSectionReader(indexName).get();
    }
    return NULL;
}

void NormalIndexReader::SetLegacyAccessoryReader(const std::shared_ptr<LegacyIndexAccessoryReader>& accessoryReader)
{
    InvertedIndexType indexType = _indexConfig->GetInvertedIndexType();
    if (indexType == it_pack || indexType == it_expack) {
        mAccessoryReader = accessoryReader;
    } else {
        mAccessoryReader.reset();
    }
}

vector<std::shared_ptr<DictionaryReader>> NormalIndexReader::GetDictReaders() const
{
    vector<std::shared_ptr<DictionaryReader>> dictReaders;
    for (size_t i = 0; i < mSegmentReaders.size(); ++i) {
        dictReaders.push_back(mSegmentReaders[i]->GetDictionaryReader());
    }
    if (mBuildingIndexReader) {
        vector<std::shared_ptr<DictionaryReader>> buildingDictReaders = mBuildingIndexReader->GetDictionaryReader();
        dictReaders.insert(dictReaders.end(), buildingDictReaders.begin(), buildingDictReaders.end());
    }
    return dictReaders;
}

void NormalIndexReader::Open(const config::IndexConfigPtr& indexConfig,
                             const index_base::PartitionDataPtr& partitionData, const InvertedIndexReader* hintReader)
{
    assert(indexConfig);

    _indexConfig = indexConfig;
    _indexFormatOption->Init(_indexConfig);
    mIndexSupportNull = _indexConfig->SupportNull();
    mTemperatureDocInfo = partitionData->GetTemperatureDocInfo();
    mDictHasher = IndexDictHasher(indexConfig->GetDictHashParams(), indexConfig->GetInvertedIndexType());
    if (indexConfig->IsIndexUpdatable() and indexConfig->GetNonTruncateIndexName().empty()) {
        // do not create dynamice index reader if index is truncateIndex
        mDynamicIndexReader = new legacy::DynamicIndexReader();
        if (!mDynamicIndexReader->Open(indexConfig, partitionData, mIndexMetrics)) {
            IE_LOG(ERROR, "Open dynamic index [%s] FAILED", indexConfig->GetIndexName().c_str());
            return;
        }
    } else if (!indexConfig->GetNonTruncateIndexName().empty()) {
        IE_LOG(DEBUG, "index [%s] non truncate index name [%s]. Do not create dynamic index reader",
               indexConfig->GetIndexName().c_str(), indexConfig->GetNonTruncateIndexName().c_str());
    }

    vector<NormalIndexSegmentReaderPtr> segmentReaders;

    if (!LoadSegments(partitionData, segmentReaders, hintReader)) {
        IE_LOG(ERROR, "Open loadSegments FAILED");
        return;
    }

    mSegmentReaders.swap(segmentReaders);

    mHighFreqVol = indexConfig->GetHighFreqVocabulary();
    if (mHighFreqVol) {
        mBitmapIndexReader = new legacy::BitmapIndexReader();
        if (!mBitmapIndexReader->Open(indexConfig, partitionData)) {
            IE_LOG(ERROR, "Open bitmap index [%s] FAILED", indexConfig->GetIndexName().c_str());
            return;
        }
    }

    // TODO: do not get executor if parallel lookup is off
    mExecutor = util::FutureExecutor::GetInternalExecutor();
    if (!mExecutor) {
        IE_LOG(DEBUG, "internal executor not created, use serial mode, index[%s]", indexConfig->GetIndexName().c_str());
    }
}

void NormalIndexReader::InitBuildResourceMetricsNode(util::BuildResourceMetrics* buildResourceMetrics)
{
    if (mDynamicIndexReader) {
        mDynamicIndexReader->InitBuildResourceMetricsNode(buildResourceMetrics);
        if (mBitmapIndexReader) {
            mBitmapIndexReader->InitBuildResourceMetricsNode(buildResourceMetrics);
        }
    }
}

bool NormalIndexReader::LoadSegments(const index_base::PartitionDataPtr& partitionData,
                                     vector<NormalIndexSegmentReaderPtr>& segmentReaders,
                                     const InvertedIndexReader* hintReader)
{
    PartitionSegmentIteratorPtr iter = partitionData->CreateSegmentIterator();
    assert(iter);
    unordered_map<segmentid_t, NormalIndexSegmentReaderPtr> hintReaderMap;
    auto typedHintReader = dynamic_cast<const NormalIndexReader*>(hintReader);
    if (typedHintReader) {
        for (auto& segReader : typedHintReader->mSegmentReaders) {
            hintReaderMap[segReader->GetSegmentData().GetSegmentId()] = segReader;
        }
    }
    while (iter->IsValid()) {
        if (iter->GetType() == SIT_BUILT) {
            SegmentData segData = iter->GetSegmentData();
            if (segData.GetSegmentInfo()->docCount == 0) {
                iter->MoveToNext();
                continue;
            }
            NormalIndexSegmentReaderPtr segmentReader;
            try {
                segmentid_t segId = segData.GetSegmentId();
                auto hintIter = hintReaderMap.find(segId);
                segmentReader = CreateSegmentReader();
                NormalIndexSegmentReader* hintSegmentReader = nullptr;
                if (hintIter != hintReaderMap.end()) {
                    hintSegmentReader = hintIter->second.get();
                }
                segmentReader->Open(GetIndexConfig(), segData, hintSegmentReader);
            } catch (const ExceptionBase& e) {
                IE_LOG(ERROR, "Load segment [%s] FAILED, reason: [%s]", segData.GetDirectory()->DebugString().c_str(),
                       e.what());
                throw;
            }
            segmentReaders.push_back(segmentReader);
        } else {
            assert(iter->GetType() == SIT_BUILDING);
            AddBuildingSegmentReader(iter->GetBaseDocId(), iter->GetInMemSegment());
            IE_LOG(DEBUG, "Add In-Memory SegmentReader for segment [%d] by index [%s]", iter->GetSegmentId(),
                   GetIndexName().c_str());
        }
        iter->MoveToNext();
    }
    return true;
}

// TODO: add ut for truncate
void NormalIndexReader::AddBuildingSegmentReader(docid_t baseDocId, const index_base::InMemorySegmentPtr& inMemSegment)
{
    if (!inMemSegment) {
        return;
    }

    string buildingIndexName = GetIndexName();
    const string& nonTruncIndexName = GetIndexConfig()->GetNonTruncateIndexName();
    if (!nonTruncIndexName.empty()) {
        buildingIndexName = nonTruncIndexName;
    }

    if (!mBuildingIndexReader) {
        mBuildingIndexReader = CreateBuildingIndexReader();
    }
    InMemorySegmentReaderPtr reader = inMemSegment->GetSegmentReader();
    const auto& indexSegReader = reader->GetMultiFieldIndexSegmentReader()->GetIndexSegmentReader(buildingIndexName);
    mBuildingIndexReader->AddSegmentReader(baseDocId, indexSegReader);
}

void NormalIndexReader::SetMultiFieldIndexReader(InvertedIndexReader* multiFieldIndexReader)
{
    mMultiFieldIndexReader = dynamic_cast<legacy::MultiFieldIndexReader*>(multiFieldIndexReader);
    assert(mMultiFieldIndexReader);
}

std::shared_ptr<BuildingIndexReader> NormalIndexReader::CreateBuildingIndexReader()
{
    return std::shared_ptr<BuildingIndexReader>(new BuildingIndexReader(_indexFormatOption->GetPostingFormatOption()));
}

NormalIndexSegmentReaderPtr NormalIndexReader::CreateSegmentReader()
{
    return NormalIndexSegmentReaderPtr(new NormalIndexSegmentReader);
}

size_t NormalIndexReader::GetTotalPostingFileLength() const
{
    size_t totalSize = 0;
    for (size_t i = 0; i < mSegmentReaders.size(); i++) {
        if (mSegmentReaders[i].get() != nullptr) {
            totalSize += mSegmentReaders[i]->GetPostingFileLength();
        }
    }
    return totalSize;
}

void NormalIndexReader::Update(docid_t docId, const document::ModifiedTokens& modifiedTokens)
{
    if (!mDynamicIndexReader) {
        IE_LOG(ERROR, "update doc[%d] failed, index [%s] not updatable", docId, _indexConfig->GetIndexName().c_str());
        return;
    }
    auto highFreqPostingType = _indexConfig->GetHighFrequencyTermPostingType();
    if (!mBitmapIndexReader or !mHighFreqVol or highFreqPostingType == hp_both) {
        mDynamicIndexReader->Update(docId, modifiedTokens);
    }

    document::ModifiedTokens dynamicTokens(modifiedTokens.FieldId());
    if (mBitmapIndexReader and mHighFreqVol) {
        for (size_t i = 0; i < modifiedTokens.NonNullTermSize(); ++i) {
            auto token = modifiedTokens[i];
            index::DictKeyInfo termKey(token.first);
            bool isHighFreqTerm = mHighFreqVol->Lookup(termKey);
            if (isHighFreqTerm) {
                assert(token.second != document::ModifiedTokens::Operation::NONE);
                mBitmapIndexReader->Update(docId, termKey, token.second == document::ModifiedTokens::Operation::REMOVE);
            }
            if (highFreqPostingType != hp_both and !isHighFreqTerm) {
                dynamicTokens.Push(token.second, token.first);
            }
        }
        document::ModifiedTokens::Operation nullTermOp;
        bool nullTermExists = modifiedTokens.GetNullTermOperation(&nullTermOp);
        bool isNullTermHighFreq = mHighFreqVol->Lookup(index::DictKeyInfo::NULL_TERM);
        if (isNullTermHighFreq and nullTermExists) {
            if (nullTermOp == document::ModifiedTokens::Operation::REMOVE) {
                mBitmapIndexReader->Update(docId, index::DictKeyInfo::NULL_TERM, /*isDelete*/ true);
            } else if (nullTermOp == document::ModifiedTokens::Operation::ADD) {
                mBitmapIndexReader->Update(docId, index::DictKeyInfo::NULL_TERM, /*isDelete*/ false);
            } else {
            }
        }
        if (highFreqPostingType != hp_both and !isNullTermHighFreq) {
            dynamicTokens.SetNullTermOperation(nullTermOp);
        }
    }
    if (dynamicTokens.NonNullTermSize() != 0 or dynamicTokens.NullTermExists()) {
        mDynamicIndexReader->Update(docId, dynamicTokens);
    }
}

void NormalIndexReader::Update(docid_t docId, index::DictKeyInfo termKey, bool isDelete)
{
    if (!mDynamicIndexReader) {
        IE_LOG(ERROR, "update doc[%d] failed, index [%s] not updatable", docId, _indexConfig->GetIndexName().c_str());
        return;
    }
    bool isHighFreqTerm = false;
    if (mBitmapIndexReader and mHighFreqVol) {
        isHighFreqTerm = mHighFreqVol->Lookup(termKey);
    }
    if (isHighFreqTerm) {
        mBitmapIndexReader->Update(docId, termKey, isDelete);
    }
    if (!isHighFreqTerm or _indexConfig->GetHighFrequencyTermPostingType() == hp_both) {
        mDynamicIndexReader->Update(docId, termKey, isDelete);
    }
}

bool NormalIndexReader::IsHighFreqTerm(const index::DictKeyInfo& termKey) const
{
    if (mBitmapIndexReader && mHighFreqVol) {
        return mHighFreqVol->Lookup(termKey);
    }
    return false;
}

DocIdRangeVector NormalIndexReader::MergeDocIdRanges(int32_t hintValues, const DocIdRangeVector& ranges) const
{
    DocIdRangeVector hintDocRanges;
    DocIdRangeVector ret = ranges;
    if (mTemperatureDocInfo && mTemperatureDocInfo->GetTemperatureDocIdRanges(hintValues, hintDocRanges)) {
        if (ranges.empty()) {
            return hintDocRanges;
        }
        ret = IntersectDocIdRange(hintDocRanges, ranges);
    }
    return ret;
}

DocIdRangeVector NormalIndexReader::IntersectDocIdRange(const DocIdRangeVector& currentRange,
                                                        const DocIdRangeVector& range) const
{
    size_t i = 0, j = 0;
    DocIdRangeVector targetRange;
    while (i < currentRange.size() && j < range.size()) {
        if (currentRange[i].first >= range[j].second) {
            j++;
        } else if (currentRange[i].second <= range[j].first) {
            i++;
        } else {
            docid_t leftDoc = max(currentRange[i].first, range[j].first);
            docid_t rightDoc = min(currentRange[i].second, range[j].second);
            DocIdRange docRange(leftDoc, rightDoc);
            targetRange.push_back(docRange);
            rightDoc == currentRange[i].second ? i++ : j++;
        }
    }
    return targetRange;
}

void NormalIndexReader::UpdateIndex(IndexUpdateTermIterator* iter)
{
    if (!mDynamicIndexReader) {
        IE_LOG(ERROR, "update patch index for segment [%d] failed, indexId [%d] indexName [%s] not updatable",
               iter->GetSegmentId(), iter->GetIndexId(), _indexConfig->GetIndexName().c_str());
        return;
    }
    segmentid_t targetSegmentId = iter->GetSegmentId();
    index::DictKeyInfo termKey = iter->GetTermKey();
    legacy::DynamicIndexReader::TermUpdater dynamicUpdater =
        mDynamicIndexReader->GetTermUpdater(targetSegmentId, termKey);
    legacy::BitmapIndexReader::TermUpdater bitmapUpdater;
    bool isHighFreqTerm = IsHighFreqTerm(termKey);
    if (isHighFreqTerm) {
        bitmapUpdater = mBitmapIndexReader->GetTermUpdater(targetSegmentId, termKey);
    }

    ComplexDocId doc;
    bool needUpdateDynamicIndex =
        !dynamicUpdater.Empty() && (!isHighFreqTerm || _indexConfig->GetHighFrequencyTermPostingType() == hp_both);
    while (iter->Next(&doc)) {
        if (needUpdateDynamicIndex) {
            dynamicUpdater.Update(doc.DocId(), doc.IsDelete());
        }
        if (!bitmapUpdater.Empty()) {
            bitmapUpdater.Update(doc.DocId(), doc.IsDelete());
        }
    }
}
}} // namespace indexlib::index
