#include "indexlib/partition/segment/in_memory_segment_cleaner.h"
#include "indexlib/partition/segment/in_memory_segment_container.h"
#include "indexlib/index_base/segment/in_memory_segment.h"

using namespace std;

IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, InMemorySegmentCleaner);

InMemorySegmentCleaner::InMemorySegmentCleaner(
        const InMemorySegmentContainerPtr& container)
    : mInMemSegContainer(container)
{
}

InMemorySegmentCleaner::~InMemorySegmentCleaner() 
{
}

void InMemorySegmentCleaner::Execute()
{
    if (!mInMemSegContainer)
    {
        return;
    }
    InMemorySegmentPtr inMemSeg = mInMemSegContainer->GetOldestInMemSegment();
    while (inMemSeg && inMemSeg.use_count() == 2)
    {
        IE_LOG(INFO, "evict old InMemorySegment[%d]", inMemSeg->GetSegmentId());
        mInMemSegContainer->EvictOldestInMemSegment();
        inMemSeg = mInMemSegContainer->GetOldestInMemSegment();
    }
}

IE_NAMESPACE_END(partition);

