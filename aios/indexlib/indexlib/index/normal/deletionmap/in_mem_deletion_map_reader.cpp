#include "indexlib/index/normal/deletionmap/in_mem_deletion_map_reader.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, InMemDeletionMapReader);

InMemDeletionMapReader::InMemDeletionMapReader(util::ExpandableBitmap* bitmap,
                                               index_base::SegmentInfo* segmentInfo,
                                               segmentid_t segId)
    : mBitmap(bitmap)
    , mSegmentInfo(segmentInfo)
    , mSegId(segId)
{
    assert(mBitmap);
    assert(mSegmentInfo);
}

InMemDeletionMapReader::~InMemDeletionMapReader() 
{
}

IE_NAMESPACE_END(index);

