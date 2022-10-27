#include "indexlib/index/normal/trie/trie_index_reader.h"
#include "indexlib/index/in_memory_segment_reader.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/index_meta/version.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, TrieIndexReader);

void TrieIndexReader::Open(const IndexConfigPtr& indexConfig,
                           const PartitionDataPtr& partitionData)
{    
    assert(indexConfig);
    mIndexConfig = indexConfig;

    LoadSegments(partitionData, mSegmentVector);
    mDeletionMapReader = partitionData->GetDeletionMapReader();
    // no realtime build, so do not need pass deletionmap
    bool hasDeletedDoc = mDeletionMapReader && mDeletionMapReader->GetDeletedDocCount() != 0;
    mOptimizeSearch = (mSegmentVector.size() == 1) && (!hasDeletedDoc);
}

void TrieIndexReader::LoadSegments(
        const PartitionDataPtr& partitionData, SegmentVector& segmentVector)
{
    for (PartitionData::Iterator iter = partitionData->Begin();
         iter != partitionData->End(); ++iter)
    {
        const SegmentData& segmentData = *iter;
        size_t docCount = segmentData.GetSegmentInfo().docCount;
        if (docCount == 0)
        {
            continue;
        }
        SegmentReaderPtr segmentReader(new SegmentReader);
        segmentReader->Open(mIndexConfig, segmentData);
        segmentVector.push_back(make_pair(segmentData.GetBaseDocId(), segmentReader));
    }
    InitBuildingIndexReader(partitionData);
}

size_t TrieIndexReader::EstimateLoadSize(const PartitionDataPtr& partitionData,
        const IndexConfigPtr& indexConfig, const Version& lastLoadVersion)
{
    Version version = partitionData->GetVersion();
    Version diffVersion = version - lastLoadVersion;
    size_t totalSize = 0;
    for (size_t i = 0; i < diffVersion.GetSegmentCount(); ++i)
    {
        const index_base::SegmentData& segData = 
            partitionData->GetSegmentData(diffVersion[i]);
        if (segData.GetSegmentInfo().docCount == 0)
        {
            continue;
        }
        DirectoryPtr indexDirectory = 
            segData.GetDirectory()->GetDirectory(INDEX_DIR_NAME, true);
        assert(indexDirectory);
        DirectoryPtr trieDirectory = 
            indexDirectory->GetDirectory(indexConfig->GetIndexName(), true);
        totalSize += trieDirectory->EstimateIntegratedFileMemoryUse(
                PRIMARY_KEY_DATA_FILE_NAME);
    }
    return totalSize;

}

docid_t TrieIndexReader::Lookup(const ConstString& pkStr) const
{
    if (mBuildingIndexReader)
    {
        docid_t gDocId = mBuildingIndexReader->Lookup(pkStr);
        if (gDocId != INVALID_DOCID && IsDocidValid(gDocId))
        {
            return gDocId;
        }
    }

    SegmentVector::const_reverse_iterator it = mSegmentVector.rbegin();
    for (; it != mSegmentVector.rend(); it++)
    {
        docid_t baseDocId = it->first;
        const SegmentReaderPtr& segmentReader = it->second;
        assert(segmentReader);
        docid_t localDocId = segmentReader->Lookup(pkStr);
        if (localDocId == INVALID_DOCID)
        {
            continue;
        }
        docid_t gDocId = baseDocId + localDocId;
        if (!IsDocidValid(gDocId))
        {
            //TODO:
            //for inc cover rt, rt doc deleted use inc doc
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
    while (buildingSegIter->IsValid())
    {
        InMemorySegmentPtr inMemSegment = buildingSegIter->GetInMemSegment();
        if (!inMemSegment)
        {
            buildingSegIter->MoveToNext();
            continue;
        }
        const InMemorySegmentReaderPtr& inMemSegReader = inMemSegment->GetSegmentReader();
        if (!inMemSegReader)
        {
            buildingSegIter->MoveToNext();
            continue;
        }

        index::IndexSegmentReaderPtr trieSegReader = 
            inMemSegReader->GetSingleIndexSegmentReader(mIndexConfig->GetIndexName());
        InMemTrieSegmentReaderPtr inMemTrieSegReader =
            DYNAMIC_POINTER_CAST(InMemTrieSegmentReader, trieSegReader);
        assert(inMemTrieSegReader);
        if (!mBuildingIndexReader)
        {
            mBuildingIndexReader.reset(new BuildingTrieIndexReader);
        }

        mBuildingIndexReader->AddSegmentReader(
                buildingSegIter->GetBaseDocId(), inMemTrieSegReader);
        IE_LOG(INFO, "Add In-Memory SegmentReader for segment [%d], by index [%s]",
               buildingSegIter->GetSegmentId(), mIndexConfig->GetIndexName().c_str());
        buildingSegIter->MoveToNext();
    }
}

IE_NAMESPACE_END(index);

