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
#include "indexlib/index/normal/trie/trie_index_reader.h"

#include "indexlib/index/normal/framework/multi_field_index_segment_reader.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/in_memory_segment_reader.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/index_define.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::index_base;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, TrieIndexReader);

void TrieIndexReader::Open(const IndexConfigPtr& indexConfig, const PartitionDataPtr& partitionData,
                           const InvertedIndexReader* hintReader)
{
    assert(indexConfig);
    _indexConfig = indexConfig;

    LoadSegments(partitionData, mSegmentVector, mSegmentIds, hintReader);
    mDeletionMapReader = partitionData->GetDeletionMapReader();
    // no realtime build, so do not need pass deletionmap
    bool hasDeletedDoc = mDeletionMapReader && mDeletionMapReader->GetDeletedDocCount() != 0;
    mOptimizeSearch = (mSegmentVector.size() == 1) && (!hasDeletedDoc);
}

void TrieIndexReader::LoadSegments(const PartitionDataPtr& partitionData, SegmentVector& segmentVector,
                                   vector<segmentid_t>& segmentIds, const InvertedIndexReader* hintReader)
{
    unordered_map<segmentid_t, TrieIndexSegmentReaderPtr> hintReaderMap;
    auto typedHintReader = dynamic_cast<const TrieIndexReader*>(hintReader);
    if (typedHintReader) {
        assert(typedHintReader->mSegmentIds.size() == typedHintReader->mSegmentVector.size());
        for (size_t i = 0; i < typedHintReader->mSegmentIds.size(); ++i) {
            hintReaderMap[typedHintReader->mSegmentIds[i]] = typedHintReader->mSegmentVector[i].second;
        }
    }
    for (PartitionData::Iterator iter = partitionData->Begin(); iter != partitionData->End(); ++iter) {
        const SegmentData& segmentData = *iter;
        size_t docCount = segmentData.GetSegmentInfo()->docCount;
        if (docCount == 0) {
            continue;
        }
        segmentid_t segId = segmentData.GetSegmentId();
        SegmentReaderPtr segmentReader;
        auto readerIter = hintReaderMap.find(segId);
        if (readerIter == hintReaderMap.end()) {
            segmentReader.reset(new SegmentReader);
            segmentReader->Open(std::dynamic_pointer_cast<IndexConfig>(_indexConfig), segmentData);
        } else {
            segmentReader = readerIter->second;
        }
        segmentVector.push_back(make_pair(segmentData.GetBaseDocId(), segmentReader));
        segmentIds.push_back(segId);
    }
    InitBuildingIndexReader(partitionData);
}

size_t TrieIndexReader::EstimateLoadSize(const PartitionDataPtr& partitionData, const IndexConfigPtr& indexConfig,
                                         const Version& lastLoadVersion)
{
    Version version = partitionData->GetVersion();
    Version diffVersion = version - lastLoadVersion;
    size_t totalSize = 0;
    for (size_t i = 0; i < diffVersion.GetSegmentCount(); ++i) {
        const index_base::SegmentData& segData = partitionData->GetSegmentData(diffVersion[i]);
        if (segData.GetSegmentInfo()->docCount == 0) {
            continue;
        }
        DirectoryPtr indexDirectory = segData.GetDirectory()->GetDirectory(INDEX_DIR_NAME, true);
        assert(indexDirectory);
        DirectoryPtr trieDirectory = indexDirectory->GetDirectory(indexConfig->GetIndexName(), true);
        totalSize += trieDirectory->EstimateFileMemoryUse(PRIMARY_KEY_DATA_FILE_NAME, FSOT_MEM_ACCESS);
    }
    return totalSize;
}

docid_t TrieIndexReader::Lookup(const StringView& pkStr) const
{
    if (mBuildingIndexReader) {
        docid_t gDocId = mBuildingIndexReader->Lookup(pkStr);
        if (gDocId != INVALID_DOCID && IsDocidValid(gDocId)) {
            return gDocId;
        }
    }

    SegmentVector::const_reverse_iterator it = mSegmentVector.rbegin();
    for (; it != mSegmentVector.rend(); it++) {
        docid_t baseDocId = it->first;
        const SegmentReaderPtr& segmentReader = it->second;
        assert(segmentReader);
        docid_t localDocId = segmentReader->Lookup(pkStr);
        if (localDocId == INVALID_DOCID) {
            continue;
        }
        docid_t gDocId = baseDocId + localDocId;
        if (!IsDocidValid(gDocId)) {
            // TODO:
            // for inc cover rt, rt doc deleted use inc doc
            continue;
        }
        return gDocId;
    }
    return INVALID_DOCID;
}

void TrieIndexReader::InitBuildingIndexReader(const PartitionDataPtr& partitionData)
{
    PartitionSegmentIteratorPtr segIter = partitionData->CreateSegmentIterator();
    assert(segIter);
    SegmentIteratorPtr buildingSegIter = segIter->CreateIterator(SIT_BUILDING);
    while (buildingSegIter->IsValid()) {
        InMemorySegmentPtr inMemSegment = buildingSegIter->GetInMemSegment();
        if (!inMemSegment) {
            buildingSegIter->MoveToNext();
            continue;
        }
        const InMemorySegmentReaderPtr& inMemSegReader = inMemSegment->GetSegmentReader();
        if (!inMemSegReader) {
            buildingSegIter->MoveToNext();
            continue;
        }

        index::IndexSegmentReaderPtr trieSegReader =
            inMemSegReader->GetMultiFieldIndexSegmentReader()->GetIndexSegmentReader(GetIndexName());
        InMemTrieSegmentReaderPtr inMemTrieSegReader = DYNAMIC_POINTER_CAST(InMemTrieSegmentReader, trieSegReader);
        assert(inMemTrieSegReader);
        if (!mBuildingIndexReader) {
            mBuildingIndexReader.reset(new BuildingTrieIndexReader);
        }

        mBuildingIndexReader->AddSegmentReader(buildingSegIter->GetBaseDocId(), inMemTrieSegReader);
        IE_LOG(INFO, "Add In-Memory SegmentReader for segment [%d], by index [%s]", buildingSegIter->GetSegmentId(),
               _indexConfig->GetIndexName().c_str());
        buildingSegIter->MoveToNext();
    }
}
}} // namespace indexlib::index
