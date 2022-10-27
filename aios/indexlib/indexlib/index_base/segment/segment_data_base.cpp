#include "indexlib/index_base/segment/segment_data_base.h"

using namespace std;

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(index_base, SegmentDataBase);

SegmentDataBase::SegmentDataBase()
    : mBaseDocId(INVALID_DOCID)
    , mSegmentId(INVALID_SEGMENTID)
{
}

SegmentDataBase::SegmentDataBase(const SegmentDataBase& other)
{
    *this = other; 
}

SegmentDataBase::~SegmentDataBase() 
{
}

SegmentDataBase& SegmentDataBase::operator=(const SegmentDataBase& other)
{
    if (this == &other)
    {
        return *this;
    }
    mBaseDocId = other.mBaseDocId;
    mSegmentId = other.mSegmentId;
    mSegmentDirName = other.mSegmentDirName;
    return *this;
}
IE_NAMESPACE_END(index_base);
