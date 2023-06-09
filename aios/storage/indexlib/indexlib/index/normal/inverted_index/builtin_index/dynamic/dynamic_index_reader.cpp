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
#include "indexlib/index/normal/inverted_index/builtin_index/dynamic/dynamic_index_reader.h"

#include "autil/StringUtil.h"
#include "indexlib/document/index_document/normal_document/modified_tokens.h"
#include "indexlib/file_system/file/ResourceFile.h"
#include "indexlib/index/common/DictKeyInfo.h"
#include "indexlib/index/inverted_index/BuildingIndexReader.h"
#include "indexlib/index/inverted_index/SegmentPosting.h"
#include "indexlib/index/inverted_index/format/ShortListOptimizeUtil.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryReader.h"
#include "indexlib/index/inverted_index/format/dictionary/TieredDictionaryReader.h"
#include "indexlib/index/inverted_index/patch/IndexUpdateTermIterator.h"
#include "indexlib/index/normal/framework/index_writer.h"
#include "indexlib/index/normal/framework/multi_field_index_segment_reader.h"
#include "indexlib/index/normal/inverted_index/builtin_index/dynamic/dynamic_index_posting_writer.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/in_memory_segment_reader.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"

namespace indexlib { namespace index { namespace legacy {

IE_LOG_SETUP(index, DynamicIndexReader);

DynamicIndexReader::DynamicIndexReader() : _fieldIndexMetrics(nullptr), _temperatureDocInfo(nullptr) {}
DynamicIndexReader::~DynamicIndexReader() {}

bool DynamicIndexReader::Open(const config::IndexConfigPtr& indexConfig,
                              const index_base::PartitionDataPtr& partitionData, IndexMetrics* indexMetrics)
{
    assert(indexConfig);
    _indexConfig = indexConfig;
    _indexFormatOption.Init(indexConfig);
    _temperatureDocInfo = partitionData->GetTemperatureDocInfo();
    _dictHasher = IndexDictHasher(indexConfig->GetDictHashParams(), indexConfig->GetInvertedIndexType());

    if (indexMetrics) {
        std::string metricsIndexName = indexConfig->GetIndexName();
        if (indexConfig->GetShardingType() == config::IndexConfig::IST_IS_SHARDING) {
            autil::StringUtil::replace(metricsIndexName, '@', '_');
        }
        _fieldIndexMetrics = indexMetrics->AddSingleFieldIndex(metricsIndexName);
    }

    if (!LoadSegments(partitionData)) {
        return false;
    }
    IE_LOG(INFO, "Open DynamicIndexReader[%s]", indexConfig->GetIndexName().c_str());
    return true;
}

void DynamicIndexReader::InitBuildResourceMetricsNode(util::BuildResourceMetrics* buildResourceMetrics)
{
    UpdateBuildResourceMetrics();
}

bool DynamicIndexReader::LoadSegments(const index_base::PartitionDataPtr& partitionData)
{
    _segmentDocCount.clear();
    _baseDocIds.clear();

    auto iter = partitionData->CreateSegmentIterator();
    assert(iter);
    while (iter->IsValid()) {
        if (iter->GetType() == index_base::SIT_BUILT) {
            auto segmentData = iter->GetSegmentData();
            if (segmentData.GetSegmentInfo()->docCount == 0) {
                iter->MoveToNext();
                continue;
            }
            file_system::DirectoryPtr directory = segmentData.GetIndexDirectory(_indexConfig->GetIndexName(), true);
            auto resourceFile = directory->GetResourceFile("dynamic_index.trees");
            if (!resourceFile or resourceFile->Empty()) {
                assert(!index_base::RealtimeSegmentDirectory::IsRtSegmentId(iter->GetSegmentId()));
                auto postingTable = new DynamicIndexWriter::DynamicPostingTable(HASHMAP_INIT_SIZE);
                if (_indexConfig->SupportNull()) {
                    postingTable->nullTermPosting = DynamicIndexWriter::CreatePostingWriter(postingTable);
                }
                resourceFile = directory->CreateResourceFile("dynamic_index.trees");
                assert(resourceFile);
                resourceFile->Reset(postingTable);
            }
            _postingResources.push_back(resourceFile);
            _segmentDocCount.push_back((uint64_t)segmentData.GetSegmentInfo()->docCount);
            _segmentIds.push_back(iter->GetSegmentId());
            _baseDocIds.push_back(iter->GetBaseDocId());
        } else {
            assert(iter->GetType() == index_base::SIT_BUILDING);
            AddInMemSegment(iter->GetBaseDocId(), iter->GetInMemSegment());
            IE_LOG(DEBUG, "Add In-Memory SegmentReader for segment [%d], by index [%s]", iter->GetSegmentId(),
                   _indexConfig->GetIndexName().c_str());
        }
        iter->MoveToNext();
    }
    return true;
}

DynamicPostingIterator* DynamicIndexReader::Lookup(const index::DictKeyInfo& key, const DocIdRangeVector& docIdRanges,
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

void DynamicIndexReader::FillSegmentContextsByRanges(
    const index::DictKeyInfo& key, const DocIdRangeVector& ranges, autil::mem_pool::Pool* sessionPool,
    std::vector<DynamicPostingIterator::SegmentContext>& segmentContexts) const
{
    size_t currentRangeIdx = 0;
    size_t currentSegmentIdx = 0;
    bool currentSegmentFilled = false;
    while (currentSegmentIdx < _postingResources.size() && currentRangeIdx < ranges.size()) {
        const auto& range = ranges[currentRangeIdx];
        docid_t segBegin = _baseDocIds[currentSegmentIdx];
        docid_t segEnd = _baseDocIds[currentSegmentIdx] + _segmentDocCount[currentSegmentIdx];
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
                auto postingTable = resource->GetResource<DynamicIndexWriter::DynamicPostingTable>();
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
                    ctx.tree = static_cast<DynamicIndexPostingWriter*>(writer)->GetPostingTree();
                    segmentContexts.push_back(ctx);
                }
            }
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
    if (currentRangeIdx < ranges.size() && _buildingIndexReader) {
        std::vector<SegmentPosting> segPostings;
        _buildingIndexReader->GetSegmentPosting(key, segPostings, sessionPool);
        for (auto& segPosting : segPostings) {
            assert(segPosting.IsRealTimeSegment());
            auto postingWriter = segPosting.GetInMemPostingWriter();
            auto dynamicPostingWriter = static_cast<const DynamicIndexPostingWriter*>(postingWriter);
            DynamicPostingIterator::SegmentContext ctx;
            ctx.baseDocId = segPosting.GetBaseDocId();
            ctx.tree = dynamicPostingWriter->GetPostingTree();
            segmentContexts.push_back(ctx);
        }
    }
}

void DynamicIndexReader::FillSegmentContexts(const index::DictKeyInfo& key, autil::mem_pool::Pool* sessionPool,
                                             std::vector<DynamicPostingIterator::SegmentContext>& segmentContexts) const
{
    for (size_t i = 0; i < _postingResources.size(); ++i) {
        auto& resource = _postingResources[i];
        if (resource and !resource->Empty()) {
            auto postingTable = resource->GetResource<DynamicIndexWriter::DynamicPostingTable>();
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
                ctx.tree = static_cast<DynamicIndexPostingWriter*>(writer)->GetPostingTree();
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
            auto dynamicPostingWriter = static_cast<const DynamicIndexPostingWriter*>(postingWriter);
            DynamicPostingIterator::SegmentContext ctx;
            ctx.baseDocId = segPosting.GetBaseDocId();
            ctx.tree = dynamicPostingWriter->GetPostingTree();
            segmentContexts.push_back(ctx);
        }
    }
}

void DynamicIndexReader::AddInMemSegment(docid_t baseDocId, const index_base::InMemorySegmentPtr& inMemSegment)
{
    if (!inMemSegment) {
        return;
    }

    index_base::InMemorySegmentReaderPtr reader = inMemSegment->GetSegmentReader();
    const auto& segmentReader =
        reader->GetMultiFieldIndexSegmentReader()->GetIndexSegmentReader(_indexConfig->GetIndexName());
    AddInMemSegmentReader(baseDocId, segmentReader->GetInMemDynamicSegmentReader());
}

void DynamicIndexReader::AddInMemSegmentReader(docid_t baseDocId,
                                               const InMemDynamicIndexSegmentReaderPtr& inMemSegReader)
{
    if (!_buildingIndexReader) {
        LegacyIndexFormatOption indexFormatOption;
        indexFormatOption.Init(_indexConfig);
        _buildingIndexReader.reset(new BuildingIndexReader(indexFormatOption.GetPostingFormatOption()));
    }
    _buildingIndexReader->AddSegmentReader(baseDocId, inMemSegReader);
}

void DynamicIndexReader::UpdateBuildResourceMetrics()
{
    size_t poolMemory = 0;
    size_t totalMemory = 0;
    size_t totalRetiredMemory = 0;
    size_t totalDocCount = 0;
    size_t totalAlloc = 0;
    size_t totalFree = 0;
    size_t totalTreeCount = 0;
    for (const auto& resource : _postingResources) {
        if (resource and !resource->Empty()) {
            auto postingTable = resource->GetResource<DynamicIndexWriter::DynamicPostingTable>();
            assert(postingTable);
            poolMemory += postingTable->pool.getUsedBytes();
            totalMemory += postingTable->nodeManager.TotalMemory();
            totalRetiredMemory += postingTable->nodeManager.RetiredMemory();
            totalAlloc += postingTable->nodeManager.TotalAllocatedMemory();
            totalFree += postingTable->nodeManager.TotalFreedMemory();
            totalTreeCount += postingTable->table.Size();
            totalDocCount += postingTable->stat.totalDocCount;

            if (postingTable->nullTermPosting) {
                ++totalTreeCount;
            }
            resource->UpdateMemoryUse(postingTable->nodeManager.TotalMemory());
        }
    }
    if (_fieldIndexMetrics) {
        _fieldIndexMetrics->dynamicIndexDocCount = totalDocCount;
        _fieldIndexMetrics->dynamicIndexMemory = totalMemory;
        _fieldIndexMetrics->dynamicIndexRetiredMemory = totalRetiredMemory;
        _fieldIndexMetrics->dynamicIndexTotalAllocatedMemory = totalAlloc;
        _fieldIndexMetrics->dynamicIndexTotalFreedMemory = totalFree;
        _fieldIndexMetrics->dynamicIndexTreeCount = totalTreeCount;
    }
}

void DynamicIndexReader::Update(docid_t docId, const document::ModifiedTokens& tokens)
{
    docid_t baseDocId = 0;
    DynamicIndexWriter::DynamicPostingTable* postingTable = nullptr;
    for (size_t i = 0; i < _segmentDocCount.size(); i++) {
        uint64_t docCount = _segmentDocCount[i];
        if (docId < baseDocId + (docid_t)docCount) {
            if (_postingResources[i] and !_postingResources[i]->Empty()) {
                postingTable = _postingResources[i]->GetResource<DynamicIndexWriter::DynamicPostingTable>();
                assert(postingTable);
            }
            break;
        }
        baseDocId += docCount;
    }
    if (!postingTable) {
        if (_buildingIndexReader) {
            _buildingIndexReader->Update(docId, tokens);
        }
        return;
    }
    docid_t localDocId = docId - baseDocId;
    for (size_t i = 0; i < tokens.NonNullTermSize(); ++i) {
        auto item = tokens[i];
        DynamicIndexWriter::UpdateToken(index::DictKeyInfo(item.first), _indexFormatOption.IsNumberIndex(),
                                        postingTable, localDocId, item.second);
    }
    document::ModifiedTokens::Operation nullTermOp;
    if (tokens.GetNullTermOperation(&nullTermOp)) {
        DynamicIndexWriter::UpdateToken(index::DictKeyInfo::NULL_TERM, _indexFormatOption.IsNumberIndex(), postingTable,
                                        localDocId, nullTermOp);
    }
    UpdateBuildResourceMetrics();
}

void DynamicIndexReader::Update(docid_t docId, const index::DictKeyInfo& key, bool isDelete)
{
    docid_t baseDocId = 0;
    DynamicIndexWriter::DynamicPostingTable* postingTable = nullptr;
    for (size_t i = 0; i < _segmentDocCount.size(); i++) {
        uint64_t docCount = _segmentDocCount[i];
        if (docId < baseDocId + (docid_t)docCount) {
            if (_postingResources[i] and !_postingResources[i]->Empty()) {
                postingTable = _postingResources[i]->GetResource<DynamicIndexWriter::DynamicPostingTable>();
                assert(postingTable);
            }
            break;
        }
        baseDocId += docCount;
    }
    if (!postingTable) {
        if (_buildingIndexReader) {
            _buildingIndexReader->Update(docId, key, isDelete);
        }
        return;
    }
    docid_t localDocId = docId - baseDocId;
    auto op = isDelete ? document::ModifiedTokens::Operation::REMOVE : document::ModifiedTokens::Operation::ADD;
    DynamicIndexWriter::UpdateToken(key, _indexFormatOption.IsNumberIndex(), postingTable, localDocId, op);
    UpdateBuildResourceMetrics();
}

DynamicIndexReader::TermUpdater DynamicIndexReader::GetTermUpdater(segmentid_t targetSegmentId,
                                                                   index::DictKeyInfo termKey)
{
    DynamicIndexWriter::DynamicPostingTable* postingTable = nullptr;
    for (size_t i = 0; i < _segmentIds.size(); ++i) {
        if (_segmentIds[i] == targetSegmentId) {
            if (_postingResources[i] and !_postingResources[i]->Empty()) {
                postingTable = _postingResources[i]->GetResource<DynamicIndexWriter::DynamicPostingTable>();
                assert(postingTable);
            }
            break;
        }
    }
    // term updater used for patch only
    return TermUpdater(termKey, _indexFormatOption.IsNumberIndex(), postingTable, this);
}

DynamicIndexReader::TermUpdater::TermUpdater(index::DictKeyInfo termKey, bool isNumberIndex,
                                             DynamicIndexWriter::DynamicPostingTable* postingTable,
                                             DynamicIndexReader* reader)
    : _termKey(termKey)
    , _isNumberIndex(isNumberIndex)
    , _postingTable(postingTable)
    , _reader(reader)
{
}

DynamicIndexReader::TermUpdater::TermUpdater(DynamicIndexReader::TermUpdater&& other)
    : _termKey(other._termKey)
    , _isNumberIndex(other._isNumberIndex)
    , _postingTable(std::exchange(other._postingTable, nullptr))
    , _reader(std::exchange(other._reader, nullptr))
{
}

DynamicIndexReader::TermUpdater& DynamicIndexReader::TermUpdater::operator=(DynamicIndexReader::TermUpdater&& other)
{
    if (&other != this) {
        _termKey = other._termKey;
        _isNumberIndex = other._isNumberIndex;
        _postingTable = std::exchange(other._postingTable, nullptr);
        _reader = std::exchange(other._reader, nullptr);
    }
    return *this;
}

DynamicIndexReader::TermUpdater::~TermUpdater()
{
    if (_reader) {
        _reader->UpdateBuildResourceMetrics();
    }
}

void DynamicIndexReader::TermUpdater::Update(docid_t localDocId, bool isDelete)
{
    if (_postingTable) {
        auto op = isDelete ? document::ModifiedTokens::Operation::REMOVE : document::ModifiedTokens::Operation::ADD;
        DynamicIndexWriter::UpdateToken(_termKey, _isNumberIndex, _postingTable, localDocId, op);
    }
}
}}} // namespace indexlib::index::legacy
