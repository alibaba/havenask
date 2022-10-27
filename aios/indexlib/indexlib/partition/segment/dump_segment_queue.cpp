#include "indexlib/partition/segment/dump_segment_queue.h"
#include "indexlib/partition/segment/custom_segment_dump_item.h"
#include "indexlib/table/building_segment_reader.h"
#include "indexlib/table/table_writer.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(table);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, DumpSegmentQueue);

DumpSegmentQueue::DumpSegmentQueue()
{
}

DumpSegmentQueue::DumpSegmentQueue(const DumpSegmentQueue& other)
    : mDumpItems(other.mDumpItems)
{
}

DumpSegmentQueue::~DumpSegmentQueue()
{
}

DumpSegmentQueue*  DumpSegmentQueue::Clone()
{
    ScopedLock lock(mLock);
    return new DumpSegmentQueue(*this);
}

void DumpSegmentQueue::PushDumpItem(const CustomSegmentDumpItemPtr& dumpItem)
{
    ScopedLock lock(mLock);
    mDumpItems.emplace_back(dumpItem);
}

void DumpSegmentQueue::GetDumpedItems(std::vector<CustomSegmentDumpItemPtr>& dumpedItems) const
{
    ScopedLock lock(mLock);
    dumpedItems.reserve(mDumpItems.size());
    for (auto item : mDumpItems)
    {
        if (item->IsDumped())
        {
            dumpedItems.push_back(item);
        }
    }
}

void DumpSegmentQueue::PopDumpedItems()
{
    ScopedLock lock(mLock);

    while (!mDumpItems.empty())
    {
        auto& item = mDumpItems.front();
        if (!item->IsDumped())
        {
            return;
        }
        mDumpItems.pop_front();
    }
}

void DumpSegmentQueue::GetBuildingSegmentReaders(
    vector<table::BuildingSegmentReaderPtr>& dumpingSegmentReaders,
    int64_t reclaimTimestamp)
{
    ScopedLock lock(mLock);
    for (const auto& item : mDumpItems)
    {
        if (reclaimTimestamp >= 0 && item->GetSegmentInfo()->timestamp < reclaimTimestamp)
        {
            continue;
        }
        auto tableWriter = item->GetTableWriter();
        auto segmentReader = tableWriter->GetBuildingSegmentReader();
        dumpingSegmentReaders.emplace_back(segmentReader);
    }
}

bool DumpSegmentQueue::ProcessOneItem(bool& hasNewDumpedSegment)
{
    hasNewDumpedSegment = false;
    CustomSegmentDumpItemPtr oldestItem;
    {
        ScopedLock lock(mLock);
        if (mDumpItems.empty())
        {
            return true;
        }
        oldestItem = mDumpItems.front();
    }
    {
        ScopedLock lock(mDumpLock);
        if (oldestItem->IsDumped())
        {
            hasNewDumpedSegment = false;
            return true;
        }
        try
        {
            oldestItem->Dump();
        }
        catch (const misc::ExceptionBase& e)
        {
            IE_LOG(ERROR, "flush dump segment[%d] end. "
                   "because catch exception:[%s].", oldestItem->GetSegmentId(), e.what());
            return false;
        }
        hasNewDumpedSegment = true;
        return true;            
    }
}

uint64_t DumpSegmentQueue::GetDumpingSegmentsSize() const 
{
    ScopedLock lock(mLock);
    uint64_t totalSize = 0;
    for (const auto& item : mDumpItems)
    {
        totalSize += (item->GetEstimateDumpSize() + item->GetInMemoryMemUse());
    }
    return totalSize;
}

size_t DumpSegmentQueue::GetEstimateDumpSize() const
{
    ScopedLock lock(mLock);
    size_t totalSize = 0;
    for (const auto& item : mDumpItems)
    {
        totalSize += item->GetEstimateDumpSize();
    }
    return totalSize;
}

CustomSegmentDumpItemPtr DumpSegmentQueue::GetLastSegment()
{
    ScopedLock lock(mLock);
    if (!mDumpItems.empty())
    {
        return mDumpItems.back();
    }
    return CustomSegmentDumpItemPtr();
}

void DumpSegmentQueue::ReclaimSegments(int64_t reclaimTimestamp)
{
    ScopedLock lock(mLock);
    if (reclaimTimestamp < 0)
    {
        return;
    }
    vector<segmentid_t> toReclaimRtSegIds;
    toReclaimRtSegIds.reserve(mDumpItems.size());
    for (auto iter = mDumpItems.begin(); iter != mDumpItems.end(); )
    {
        if ((*iter)->GetSegmentInfo()->timestamp < reclaimTimestamp)
        {
            toReclaimRtSegIds.emplace_back((*iter)->GetSegmentId());
            iter = mDumpItems.erase(iter);
            continue;
        }
        ++iter;
    }
    if (!toReclaimRtSegIds.empty())
    {
        string segListStr = StringUtil::toString(toReclaimRtSegIds, ",");
        IE_LOG(INFO, "dumpingSegments: %s will be reclaimed with relcaimTs[%ld]",
               segListStr.c_str(), reclaimTimestamp );
    } 
}


IE_NAMESPACE_END(partition);

