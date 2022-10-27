#include "indexlib/partition/segment/in_memory_segment_releaser.h"
#include "indexlib/index_base/segment/in_memory_segment.h"

using namespace std;

IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, InMemorySegmentReleaser);

InMemorySegmentReleaser::InMemorySegmentReleaser(
        const InMemorySegmentPtr& inMemorySegment)
    : mInMemorySegment(inMemorySegment)
{
}

InMemorySegmentReleaser::~InMemorySegmentReleaser() 
{
}

void InMemorySegmentReleaser::process()
{
    assert(mInMemorySegment.use_count() == 1);
    mInMemorySegment.reset();
}

void InMemorySegmentReleaser::destroy()
{
    delete this;
}

void InMemorySegmentReleaser::drop()
{
    destroy();
}


IE_NAMESPACE_END(partition);

