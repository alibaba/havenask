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
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapIndexReader.h"

#include "indexlib/config/IIndexConfig.h"
#include "indexlib/file_system/file/ResourceFile.h"
#include "indexlib/index/inverted_index/BuildingIndexReader.h"
#include "indexlib/index/inverted_index/IndexSegmentReader.h"
#include "indexlib/index/inverted_index/InvertedIndexSearchTracer.h"
#include "indexlib/index/inverted_index/InvertedIndexUtil.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapIndexWriter.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapLeafReader.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingExpandData.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingIterator.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/InMemBitmapIndexSegmentReader.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, BitmapIndexReader);

BitmapIndexReader::BitmapIndexReader() {}

BitmapIndexReader::~BitmapIndexReader() {}

Status BitmapIndexReader::Open(
    const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
    const std::vector<std::pair<docid64_t, std::shared_ptr<BitmapLeafReader>>>& diskSegmentReaders,
    const std::vector<std::pair<docid64_t, std::shared_ptr<InMemBitmapIndexSegmentReader>>>& memSegmentReaders)
{
    _indexConfig = indexConfig;
    _indexFormatOption.Init(_indexConfig);
    _dictHasher = IndexDictHasher(_indexConfig->GetDictHashParams(), _indexConfig->GetInvertedIndexType());
    std::vector<std::shared_ptr<BitmapLeafReader>> segmentReaders;
    std::vector<docid64_t> baseDocIds;
    for (auto& [baseDocId, diskReader] : diskSegmentReaders) {
        segmentReaders.emplace_back(diskReader);
        baseDocIds.emplace_back(baseDocId);
    }
    for (auto& [baseDocId, memReader] : memSegmentReaders) {
        AddInMemSegmentReader(baseDocId, memReader);
    }
    _segmentReaders.swap(segmentReaders);
    _baseDocIds.swap(baseDocIds);
    return Status::OK();
}

void BitmapIndexReader::AddInMemSegmentReader(docid64_t baseDocId,
                                              const std::shared_ptr<InMemBitmapIndexSegmentReader>& bitmapSegReader)
{
    if (!_buildingIndexReader) {
        IndexFormatOption indexFormatOption;
        indexFormatOption.Init(_indexConfig);
        PostingFormatOption bitmapPostingFormatOption =
            indexFormatOption.GetPostingFormatOption().GetBitmapPostingFormatOption();
        _buildingIndexReader.reset(new BuildingIndexReader(bitmapPostingFormatOption));
    }
    _buildingIndexReader->AddSegmentReader(baseDocId, bitmapSegReader);
}

index::Result<PostingIterator*> BitmapIndexReader::Lookup(const index::DictKeyInfo& key, const DocIdRangeVector& ranges,
                                                          uint32_t statePoolSize, autil::mem_pool::Pool* sessionPool,
                                                          file_system::ReadOption option) const noexcept

{
    IndexFormatOption indexFormatOption;
    indexFormatOption.Init(_indexConfig);
    auto bitmapPostingFormatOption = indexFormatOption.GetPostingFormatOption().GetBitmapPostingFormatOption();

    auto tracer = util::MakePooledUniquePtr<InvertedIndexSearchTracer>(sessionPool);
    tracer->SetBitmapTerm();

    std::shared_ptr<SegmentPostingVector> segPostings(new SegmentPostingVector);
    auto ec = FillSegmentPostingVector(key, ranges, sessionPool, bitmapPostingFormatOption, segPostings, option,
                                       tracer.get());
    if (ec != index::ErrorCode::OK) {
        return ec;
    }

    BitmapPostingIterator* bitmapIt = CreateBitmapPostingIterator(sessionPool, std::move(tracer));
    assert(bitmapIt);
    if (bitmapIt->Init(segPostings, statePoolSize)) {
        return bitmapIt;
    }
    IE_POOL_COMPATIBLE_DELETE_CLASS(sessionPool, bitmapIt);
    return nullptr;
}

BitmapPostingIterator*
BitmapIndexReader::CreateBitmapPostingIterator(autil::mem_pool::Pool* sessionPool,
                                               util::PooledUniquePtr<InvertedIndexSearchTracer> tracer) const
{
    return IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, BitmapPostingIterator, _indexConfig->GetOptionFlag(), sessionPool,
                                        std::move(tracer));
}

index::ErrorCode BitmapIndexReader::FillSegmentPostingVector(
    const index::DictKeyInfo& key, const DocIdRangeVector& ranges, autil::mem_pool::Pool* sessionPool,
    PostingFormatOption bitmapPostingFormatOption, std::shared_ptr<SegmentPostingVector>& segPostings,
    file_system::ReadOption option, InvertedIndexSearchTracer* tracer) const noexcept
{
    size_t reserveCount = _segmentReaders.size();
    size_t buildingSegCount = 0;
    if (_buildingIndexReader) {
        buildingSegCount = _buildingIndexReader->GetSegmentCount();
        reserveCount += buildingSegCount;
    }
    segPostings->reserve(reserveCount);
    if (tracer) {
        tracer->SetSearchedSegmentCount(reserveCount);
        tracer->SetSearchedInMemSegmentCount(buildingSegCount);
    }
    if (ranges.empty()) {
        for (uint32_t i = 0; i < _segmentReaders.size(); i++) {
            SegmentPosting segPosting(bitmapPostingFormatOption);
            auto segmentResult = _segmentReaders[i]->GetSegmentPosting(key, _baseDocIds[i], segPosting, option, tracer);
            if (!segmentResult.Ok()) {
                return segmentResult.GetErrorCode();
            }
            if (segmentResult.Value()) {
                segPostings->push_back(std::move(segPosting));
            }
        }
        if (_buildingIndexReader) {
            _buildingIndexReader->GetSegmentPosting(key, *segPostings, sessionPool, tracer);
        }
    } else {
        size_t currentRangeIdx = 0;
        size_t currentSegmentIdx = 0;
        bool currentSegmentFilled = false;
        while (currentSegmentIdx < _segmentReaders.size() && currentRangeIdx < ranges.size()) {
            const auto& range = ranges[currentRangeIdx];
            docid64_t segBegin = _baseDocIds[currentSegmentIdx];
            docid64_t segEnd = _baseDocIds[currentSegmentIdx] + _segmentReaders[currentSegmentIdx]->GetDocCount();
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
                SegmentPosting segPosting(bitmapPostingFormatOption);
                auto segmentResult = _segmentReaders[currentSegmentIdx]->GetSegmentPosting(
                    key, _baseDocIds[currentSegmentIdx], segPosting, option, tracer);
                if (!segmentResult.Ok()) {
                    return segmentResult.GetErrorCode();
                }
                if (segmentResult.Value()) {
                    segPostings->push_back(std::move(segPosting));
                }
                currentSegmentFilled = true;
            }

            auto minEnd = std::min(segEnd, (docid64_t)range.second);
            if (segEnd == minEnd) {
                ++currentSegmentIdx;
                currentSegmentFilled = false;
            }
            if (range.second == minEnd) {
                ++currentRangeIdx;
            }
        }
        if (currentRangeIdx < ranges.size() && _buildingIndexReader) {
            _buildingIndexReader->GetSegmentPosting(key, *segPostings, sessionPool, tracer);
        }
    }
    return index::ErrorCode::OK;
}

index::Result<bool> BitmapIndexReader::GetSegmentPosting(const index::DictKeyInfo& key, uint32_t segmentIdx,
                                                         SegmentPosting& segPosting, file_system::ReadOption option,
                                                         InvertedIndexSearchTracer* tracer) const noexcept
{
    auto getResult =
        _segmentReaders[segmentIdx]->GetSegmentPosting(key, _baseDocIds[segmentIdx], segPosting, option, tracer);
    if (!getResult.Ok()) {
        AUTIL_LOG(ERROR, "get segment posting fail, indexName[%s] segmentIdx[%u] errorCode[%d]",
                  _indexConfig->GetIndexName().c_str(), segmentIdx, (int)getResult.GetErrorCode());
        return getResult.GetErrorCode();
    }
    bool found = getResult.Value();
    auto expandResource = _segmentReaders[segmentIdx]->GetExpandResource();
    if (expandResource && !expandResource->Empty()) {
        auto dataTable = expandResource->GetResource<ExpandDataTable>();
        assert(dataTable);
        BitmapPostingExpandData** pData = nullptr;
        if (key.IsNull()) {
            pData = &(dataTable->nullTermData);
        } else {
            auto retrievalHashKey =
                InvertedIndexUtil::GetRetrievalHashKey(_indexFormatOption.IsNumberIndex(), key.GetKey());
            pData = dataTable->table.Find(retrievalHashKey);
        }
        if (pData) {
            if (!found) {
                segPosting.Init(_baseDocIds[segmentIdx], _segmentDocCount[segmentIdx]);
            }
            segPosting.SetResource(*pData);
            found = true;
        }
    }
    return found;
}

} // namespace indexlib::index
