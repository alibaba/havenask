#include <fslib/fslib.h>
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_define.h"
#include "indexlib/index/in_memory_segment_reader.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/index_base/segment/in_memory_segment.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, DeletionMapReader);

DeletionMapReader::DeletionMapReader() 
    : mWriter(false)
    , mInMemBaseDocId(0)
{
}

DeletionMapReader::DeletionMapReader(uint32_t totalDocCount)
    : mWriter(false)
    , mInMemBaseDocId(totalDocCount)
{
    mBitmap.reset(new Bitmap(totalDocCount));
}

DeletionMapReader::~DeletionMapReader() 
{
}

bool DeletionMapReader::Open(PartitionData* partitionData)
{
    assert(partitionData);
    mWriter.Init(partitionData);
    mBitmap = mWriter.CreateGlobalBitmap();
    mInMemBaseDocId = mBitmap->GetItemCount();
    InitSegmentMetaMap(partitionData);

    InMemorySegmentPtr inMemSegment = partitionData->GetInMemorySegment();
    if (inMemSegment)
    {
        mInMemDeletionMapReader = 
            inMemSegment->GetSegmentReader()->GetInMemDeletionMapReader();
    }
    return true;
}

void DeletionMapReader::InitSegmentMetaMap(
        PartitionData* partitionData)
{
    PartitionSegmentIteratorPtr iter = partitionData->CreateSegmentIterator();
    assert(iter);
    while (iter->IsValid())
    {
        if (iter->GetType() == SIT_BUILDING
            && iter->GetInMemSegment()->GetStatus() == InMemorySegment::BUILDING)
        {
            // building segment
            break;
        }
        segmentid_t segId = iter->GetSegmentId();
        docid_t baseDocid = iter->GetBaseDocId();
        mSegmentMetaMap[segId] = 
            make_pair(baseDocid, iter->GetSegmentData().GetSegmentInfo().docCount);
        iter->MoveToNext();
    }
}

uint32_t DeletionMapReader::GetDeletedDocCount(segmentid_t segId) const
{
    SegmentMetaMap::const_iterator iter = mSegmentMetaMap.find(segId);
    if (iter != mSegmentMetaMap.end())
    {
        SegmentMeta segMeta = iter->second;
        docid_t baseDocid = segMeta.first;
        uint32_t docCount = segMeta.second;

        if (docCount == 0)
        {
            return 0;
        }

        return mBitmap->GetSetCount((uint32_t)baseDocid,
                baseDocid + docCount - 1);
    }

    if (mInMemDeletionMapReader &&
        segId == mInMemDeletionMapReader->GetInMemSegmentId())
    {
        return mInMemDeletionMapReader->GetDeletedDocCount();
    }
    return 0;
}

const ExpandableBitmap* DeletionMapReader::GetSegmentDeletionMap(segmentid_t segId) const
{
    return mWriter.GetSegmentDeletionMap(segId);
}

IE_NAMESPACE_END(index);
