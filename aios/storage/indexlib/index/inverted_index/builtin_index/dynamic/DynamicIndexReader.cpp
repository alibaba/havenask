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
#include "indexlib/index/inverted_index/builtin_index/dynamic/DynamicIndexReader.h"

#include "autil/StringUtil.h"
#include "indexlib/document/normal/ModifiedTokens.h"
#include "indexlib/file_system/file/ResourceFile.h"
#include "indexlib/index/common/DictKeyInfo.h"
#include "indexlib/index/inverted_index/BuildingIndexReader.h"
#include "indexlib/index/inverted_index/InvertedIndexUtil.h"
#include "indexlib/index/inverted_index/SegmentPosting.h"
#include "indexlib/index/inverted_index/builtin_index/dynamic/DynamicPostingWriter.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"
#include "indexlib/util/PoolUtil.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"

namespace indexlib::index {
namespace {
using indexlibv2::config::IIndexConfig;
using indexlibv2::config::InvertedIndexConfig;
using indexlibv2::framework::TabletData;
} // namespace

AUTIL_LOG_SETUP(indexlib.index, DynamicIndexReader);

DynamicIndexReader::DynamicIndexReader() {}
DynamicIndexReader::~DynamicIndexReader() {}

void DynamicIndexReader::Open(
    const std::shared_ptr<InvertedIndexConfig>& indexConfig,
    std::vector<std::tuple</*baseDocId=*/docid64_t, /*segmentDocCount=*/uint64_t,
                           /*dynamicPostingResourceFile=*/std::shared_ptr<indexlib::file_system::ResourceFile>>>
        dynamicPostingResources,
    std::vector<std::pair<docid64_t, std::shared_ptr<DynamicIndexSegmentReader>>> dynamicSegmentReaders)
{
    _indexConfig = indexConfig;
    assert(_indexConfig != nullptr);
    _indexFormatOption.Init(_indexConfig);
    _dictHasher = IndexDictHasher(_indexConfig->GetDictHashParams(), _indexConfig->GetInvertedIndexType());

    LoadSegments(dynamicPostingResources, dynamicSegmentReaders);
    AUTIL_LOG(INFO, "Open DynamicIndexReader [%s]", _indexConfig->GetIndexName().c_str());
}

void DynamicIndexReader::FillSegmentContextsByRanges(
    const DictKeyInfo& key, const DocIdRangeVector& ranges, autil::mem_pool::Pool* sessionPool,
    std::vector<DynamicPostingIterator::SegmentContext>& segmentContexts) const
{
    size_t currentRangeIdx = 0;
    size_t currentSegmentIdx = 0;
    bool currentSegmentFilled = false;
    while (currentSegmentIdx < _postingResources.size() && currentRangeIdx < ranges.size()) {
        const auto& range = ranges[currentRangeIdx];
        docid64_t segBegin = _baseDocIds[currentSegmentIdx];
        docid64_t segEnd = _baseDocIds[currentSegmentIdx] + _segmentDocCount[currentSegmentIdx];
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
            auto& resource = _postingResources[currentSegmentIdx];
            if (resource and !resource->Empty()) {
                auto postingTable = resource->GetResource<DynamicMemIndexer::DynamicPostingTable>();
                assert(postingTable);
                PostingWriter* writer = nullptr;
                if (key.IsNull()) {
                    writer = postingTable->nullTermPosting;
                } else {
                    PostingWriter** pWriter = postingTable->table.Find(
                        InvertedIndexUtil::GetRetrievalHashKey(_indexFormatOption.IsNumberIndex(), key.GetKey()));
                    if (pWriter) {
                        writer = *pWriter;
                    }
                }
                if (writer) {
                    DynamicPostingIterator::SegmentContext ctx;
                    ctx.baseDocId = _baseDocIds[currentSegmentIdx];
                    ctx.tree = static_cast<DynamicPostingWriter*>(writer)->GetPostingTree();
                    segmentContexts.push_back(ctx);
                }
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
        std::vector<SegmentPosting> segPostings;
        _buildingIndexReader->GetSegmentPosting(key, segPostings, sessionPool);
        for (auto& segPosting : segPostings) {
            assert(segPosting.IsRealTimeSegment());
            auto postingWriter = segPosting.GetInMemPostingWriter();
            auto dynamicPostingWriter = static_cast<const DynamicPostingWriter*>(postingWriter);
            DynamicPostingIterator::SegmentContext ctx;
            ctx.baseDocId = segPosting.GetBaseDocId();
            ctx.tree = dynamicPostingWriter->GetPostingTree();
            segmentContexts.push_back(ctx);
        }
    }
}

void DynamicIndexReader::FillSegmentContexts(const DictKeyInfo& key, autil::mem_pool::Pool* sessionPool,
                                             std::vector<DynamicPostingIterator::SegmentContext>& segmentContexts) const
{
    for (size_t i = 0; i < _postingResources.size(); ++i) {
        auto& resource = _postingResources[i];
        if (resource and !resource->Empty()) {
            auto postingTable = resource->GetResource<DynamicMemIndexer::DynamicPostingTable>();
            assert(postingTable);
            PostingWriter* writer = nullptr;
            if (key.IsNull()) {
                writer = postingTable->nullTermPosting;
            } else {
                PostingWriter** pWriter = postingTable->table.Find(
                    InvertedIndexUtil::GetRetrievalHashKey(_indexFormatOption.IsNumberIndex(), key.GetKey()));
                if (pWriter) {
                    writer = *pWriter;
                }
            }
            if (writer) {
                DynamicPostingIterator::SegmentContext ctx;
                ctx.baseDocId = _baseDocIds[i];
                ctx.tree = static_cast<DynamicPostingWriter*>(writer)->GetPostingTree();
                segmentContexts.push_back(ctx);
            }
        }
    }

    if (_buildingIndexReader) {
        std::vector<SegmentPosting> segPostings;
        _buildingIndexReader->GetSegmentPosting(key, segPostings, sessionPool);
        for (auto& segPosting : segPostings) {
            assert(segPosting.IsRealTimeSegment());
            auto postingWriter = segPosting.GetInMemPostingWriter();
            auto dynamicPostingWriter = static_cast<const DynamicPostingWriter*>(postingWriter);
            DynamicPostingIterator::SegmentContext ctx;
            ctx.baseDocId = segPosting.GetBaseDocId();
            ctx.tree = dynamicPostingWriter->GetPostingTree();
            segmentContexts.push_back(ctx);
        }
    }
}

DynamicPostingIterator* DynamicIndexReader::Lookup(const DictKeyInfo& key, const DocIdRangeVector& docIdRanges,
                                                   uint32_t statePoolSize, autil::mem_pool::Pool* sessionPool) const
{
    std::vector<DynamicPostingIterator::SegmentContext> segmentContexts;
    size_t reserveCount = _postingResources.size();
    if (_buildingIndexReader) {
        reserveCount += _buildingIndexReader->GetSegmentCount();
    }
    segmentContexts.reserve(reserveCount);
    if (docIdRanges.empty()) {
        FillSegmentContexts(key, sessionPool, segmentContexts);
    } else {
        FillSegmentContextsByRanges(key, docIdRanges, sessionPool, segmentContexts);
    }
    if (segmentContexts.empty()) {
        return nullptr;
    }
    DynamicPostingIterator* iter =
        IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, DynamicPostingIterator, sessionPool, std::move(segmentContexts));
    assert(iter);
    return iter;
}

index::Result<DynamicPostingIterator*> DynamicIndexReader::Lookup(const index::Term& term, uint32_t statePoolSize,
                                                                  autil::mem_pool::Pool* sessionPool) const
{
    index::DictKeyInfo key;
    if (!_dictHasher.GetHashKey(term, key)) {
        AUTIL_LOG(WARN, "invalid term [%s], index name [%s]", term.GetWord().c_str(),
                  _indexConfig->GetIndexName().c_str());
        return nullptr;
    }
    return Lookup(key, {}, statePoolSize, sessionPool);
}

index::Result<DynamicPostingIterator*> DynamicIndexReader::PartialLookup(const index::Term& term,
                                                                         const DocIdRangeVector& ranges,
                                                                         uint32_t statePoolSize,
                                                                         autil::mem_pool::Pool* sessionPool) const
{
    index::DictKeyInfo key;
    if (!_dictHasher.GetHashKey(term, key)) {
        AUTIL_LOG(WARN, "invalid term [%s], index name [%s]", term.GetWord().c_str(),
                  _indexConfig->GetIndexName().c_str());
        return nullptr;
    }
    return Lookup(key, ranges, statePoolSize, sessionPool);
}

void DynamicIndexReader::TEST_SetIndexConfig(const std::shared_ptr<IIndexConfig>& indexConfig)
{
    _indexConfig = std::dynamic_pointer_cast<InvertedIndexConfig>(indexConfig);
    _indexFormatOption.Init(_indexConfig);
}

void DynamicIndexReader::LoadSegments(
    std::vector<std::tuple<docid64_t, uint64_t, std::shared_ptr<indexlib::file_system::ResourceFile>>>
        dynamicPostingResources,
    std::vector<std::pair<docid64_t, std::shared_ptr<DynamicIndexSegmentReader>>> dynamicSegmentReaders)
{
    _segmentDocCount.clear();
    _baseDocIds.clear();
    for (auto& [baseDocId, docCount, postingResourceFile] : dynamicPostingResources) {
        if (docCount == 0) {
            continue;
        }
        assert(postingResourceFile);
        _postingResources.push_back(postingResourceFile);
        _segmentDocCount.push_back(docCount);
        _baseDocIds.push_back(baseDocId);
    }

    for (auto& [baseDocId, dynamicSegmentReader] : dynamicSegmentReaders) {
        AddInMemSegmentReader(baseDocId, dynamicSegmentReader);
    }
}

void DynamicIndexReader::AddInMemSegmentReader(docid64_t baseDocId,
                                               const std::shared_ptr<DynamicIndexSegmentReader>& inMemSegReader)
{
    if (!_buildingIndexReader) {
        IndexFormatOption indexFormatOption;
        indexFormatOption.Init(_indexConfig);
        _buildingIndexReader.reset(new BuildingIndexReader(indexFormatOption.GetPostingFormatOption()));
    }
    _buildingIndexReader->AddSegmentReader(baseDocId, inMemSegReader);
}

} // namespace indexlib::index
