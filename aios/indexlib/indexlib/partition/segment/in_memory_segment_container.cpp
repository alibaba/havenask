#include "indexlib/partition/segment/in_memory_segment_container.h"
#include "indexlib/index_base/segment/in_memory_segment.h"

using namespace std;

IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, InMemorySegmentContainer);

const size_t InMemorySegmentContainer::INVALID_MEMORY_USE = std::numeric_limits<size_t>::max();

InMemorySegmentContainer::InMemorySegmentContainer() 
    : mTotalMemUseCache(INVALID_MEMORY_USE)
{
}

InMemorySegmentContainer::~InMemorySegmentContainer() 
{
}

void InMemorySegmentContainer::PushBack(const InMemorySegmentPtr& inMemSeg)
{
    autil::ScopedLock lock(mInMemSegVecLock);
    if (!inMemSeg)
    {
        return;
    }

    if (!mInMemSegVec.empty() && (*mInMemSegVec.rbegin()).get() == inMemSeg.get())
    {
        IE_LOG(WARN, "in mem segment [%d] already in segment container",
               inMemSeg->GetSegmentId());
        return;
    }
    mInMemSegVec.push_back(inMemSeg);
    mTotalMemUseCache = INVALID_MEMORY_USE;

}

void InMemorySegmentContainer::EvictOldestInMemSegment()
{
    autil::ScopedLock lock(mInMemSegVecLock);
    if (!mInMemSegVec.empty())
    {
        mInMemSegVec.erase(mInMemSegVec.begin());
        mTotalMemUseCache = INVALID_MEMORY_USE;
        mInMemSegVecLock.signal();
    }
}

InMemorySegmentPtr InMemorySegmentContainer::GetOldestInMemSegment() const
{
    autil::ScopedLock lock(mInMemSegVecLock);
    if (!mInMemSegVec.empty())
    {
        return *mInMemSegVec.begin();
    }
    return InMemorySegmentPtr();
}

void InMemorySegmentContainer::WaitEmpty() const
{
    autil::ScopedLock lock(mInMemSegVecLock);
    while (!mInMemSegVec.empty())
    {
        mInMemSegVecLock.wait();
    }
}

size_t InMemorySegmentContainer::GetTotalMemoryUse() const
{
    size_t memUse = mTotalMemUseCache;
    if (memUse != INVALID_MEMORY_USE)
    {
        return memUse;
    }
    memUse = 0;
    autil::ScopedLock lock(mInMemSegVecLock);
    for (size_t i = 0; i < mInMemSegVec.size(); ++i)
    {
        memUse += mInMemSegVec[i]->GetTotalMemoryUse();
    }
    mTotalMemUseCache = memUse;
    return memUse;
}

IE_NAMESPACE_END(partition);

