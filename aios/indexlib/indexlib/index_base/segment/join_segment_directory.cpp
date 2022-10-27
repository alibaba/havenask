#include "indexlib/index_base/segment/join_segment_directory.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/index_define.h"

using namespace std;
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(index_base, JoinSegmentDirectory);

JoinSegmentDirectory::JoinSegmentDirectory() 
{
}

JoinSegmentDirectory::~JoinSegmentDirectory() 
{
}

void JoinSegmentDirectory::AddSegment(segmentid_t segId, timestamp_t ts)
{
    mVersion.Clear();
    SegmentDirectory::AddSegment(segId, ts);
}

void JoinSegmentDirectory::DoInit(const file_system::DirectoryPtr& directory)
{
}

IE_NAMESPACE_END(index_base);

