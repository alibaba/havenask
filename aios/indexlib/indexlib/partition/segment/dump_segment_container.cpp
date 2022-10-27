#include "indexlib/partition/segment/dump_segment_container.h"
#include "indexlib/partition/segment/in_memory_segment_container.h"
#include "indexlib/partition/segment/normal_segment_dump_item.h"
#include "indexlib/index_base/segment/in_memory_segment.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, DumpSegmentContainer);

DumpSegmentContainer::DumpSegmentContainer()
    : mInMemSegContainer(new InMemorySegmentContainer)
{

}

DumpSegmentContainer::DumpSegmentContainer(const DumpSegmentContainer& other)
    : mDumpItems(other.mDumpItems)
    , mInMemSegContainer(other.mInMemSegContainer)
{
}

DumpSegmentContainer::~DumpSegmentContainer() 
{
}

void DumpSegmentContainer::PushDumpItem(const NormalSegmentDumpItemPtr& dumpItem)
{
    ScopedLock lock(mLock);
    dumpItem->SetReleaseOperationAfterDump(false);
    mDumpItems.push_back(dumpItem);
}

size_t DumpSegmentContainer::GetUnDumpedSegmentSize()
{
    ScopedLock lock(mLock);
    size_t count = 0;
    for(auto item: mDumpItems)
    {
        if (!item->IsDumped())
        {
            count++;
        }
    }
    return count;
}

void DumpSegmentContainer::GetDumpingSegments(std::vector<InMemorySegmentPtr>& dumpingSegments)
{
    ScopedLock lock(mLock);
    dumpingSegments.clear();
    for(auto item: mDumpItems)
    {
        dumpingSegments.push_back(item->GetInMemorySegment());
    }
}

void DumpSegmentContainer::GetSubDumpingSegments(std::vector<InMemorySegmentPtr>& dumpingSegments)
{
    ScopedLock lock(mLock);
    dumpingSegments.clear();
    for(auto item: mDumpItems)
    {
        dumpingSegments.push_back(item->GetInMemorySegment()->GetSubInMemorySegment());
    }
}

DumpSegmentContainer* DumpSegmentContainer::Clone()
{
    ScopedLock lock(mLock);
    return new DumpSegmentContainer(*this);
}

InMemorySegmentPtr DumpSegmentContainer::GetLastSegment()
{
    ScopedLock lock(mLock);
    if (mDumpItems.empty())
    {
        return InMemorySegmentPtr();
    }
    return mDumpItems.back()->GetInMemorySegment();
}


NormalSegmentDumpItemPtr DumpSegmentContainer::GetOneSegmentItemToDump()
{
    ScopedLock lock(mLock);
    if (mDumpItems.empty())
    {
        return NormalSegmentDumpItemPtr();
    }
    for (size_t i = 0; i < mDumpItems.size(); i++)
    {
        if (!mDumpItems[i]->IsDumped())
        {
            return mDumpItems[i];
        }
    }
    return NormalSegmentDumpItemPtr();
}

void DumpSegmentContainer::ClearDumpedSegment()
{
    ScopedLock lock(mLock);
    std::vector<NormalSegmentDumpItemPtr> dumpItems;
    for(auto item: mDumpItems)
    {
        if (!item->IsDumped())
        {
            dumpItems.push_back(item);
        }
    }
    mDumpItems.swap(dumpItems);
}

void DumpSegmentContainer::GetDumpedSegments(std::vector<InMemorySegmentPtr>& dumpedSegments)
{
    ScopedLock lock(mLock);
    for(auto item: mDumpItems)
    {
        if (item->IsDumped())
        {
            dumpedSegments.push_back(item->GetInMemorySegment());
        }
    }
}

void DumpSegmentContainer::TrimObsoleteDumpingSegment(int64_t ts)
{
    ScopedLock lock(mLock);
    std::vector<NormalSegmentDumpItemPtr> dumpItems;
    size_t idx = 0;
    for(idx = 0; idx < mDumpItems.size(); idx++)
    {
        if (mDumpItems[idx]->GetInMemorySegment()->GetTimestamp() > ts)
        {
            break;
        }
    }

    for (; idx < mDumpItems.size(); idx++)
    {
        dumpItems.push_back(mDumpItems[idx]);
    }
    mDumpItems.swap(dumpItems);
}

uint64_t DumpSegmentContainer::GetDumpingSegmentsSize()
{
    ScopedLock lock(mLock);
    uint64_t totalSize = 0;
    for (size_t i = 0; i < mDumpItems.size(); i++)
    {
        totalSize += mDumpItems[i]->GetEstimateDumpSize();
    }
    return totalSize;
}

uint64_t DumpSegmentContainer::GetMaxDumpingSegmentExpandMemUse()
{
    ScopedLock lock(mLock);
    uint64_t dumpingMemUse = 0;
    for (size_t i = 0; i < mDumpItems.size(); i++)
    {
        if (!mDumpItems[i]->IsDumped())
        {
            dumpingMemUse = max(dumpingMemUse, mDumpItems[i]->GetEstimateDumpSize());
        }
    }
    return dumpingMemUse;
}
IE_NAMESPACE_END(partition);

