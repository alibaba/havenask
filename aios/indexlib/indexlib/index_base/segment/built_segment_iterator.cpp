#include "indexlib/index_base/segment/built_segment_iterator.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_base/segment/in_memory_segment.h"

using namespace std;

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(index_base, BuiltSegmentIterator);

BuiltSegmentIterator::BuiltSegmentIterator()
    : mCurrentBaseDocId(INVALID_DOCID)
    , mCursor(0)
{
}

BuiltSegmentIterator::~BuiltSegmentIterator() 
{
}

bool BuiltSegmentIterator::IsValid() const 
{
    return mCursor < mSegmentDatas.size();
}
    
void BuiltSegmentIterator::MoveToNext()
{
    if (!IsValid())
    {
        return;
    }
    mCurrentBaseDocId += mSegmentDatas[mCursor].GetSegmentInfo().docCount;
    mCursor++;
}
    
InMemorySegmentPtr BuiltSegmentIterator::GetInMemSegment() const
{
    return InMemorySegmentPtr();
}
    
SegmentData BuiltSegmentIterator::GetSegmentData() const
{
    SegmentData segData = IsValid() ? mSegmentDatas[mCursor] : SegmentData();
    segData.SetBaseDocId(mCurrentBaseDocId);
    return segData;
}
    
exdocid_t BuiltSegmentIterator::GetBaseDocId() const
{
    return mCurrentBaseDocId;
}
    
segmentid_t BuiltSegmentIterator::GetSegmentId() const
{
    return IsValid() ? mSegmentDatas[mCursor].GetSegmentId() : INVALID_SEGMENTID;
}
    
SegmentIteratorType BuiltSegmentIterator::GetType() const
{
    return SIT_BUILT;
}

void BuiltSegmentIterator::Reset()
{
    mCurrentBaseDocId = 0;
    mCursor = 0;
}

void BuiltSegmentIterator::Init(const std::vector<SegmentData>& segmentDatas)
{
    mSegmentDatas = segmentDatas;
    mCurrentBaseDocId = 0;
    mCursor = 0;
}

IE_NAMESPACE_END(index_base);

