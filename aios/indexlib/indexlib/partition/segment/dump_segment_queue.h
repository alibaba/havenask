#ifndef __INDEXLIB_DUMP_SEGMENT_QUEUE_H
#define __INDEXLIB_DUMP_SEGMENT_QUEUE_H

#include <tr1/memory>
#include <list>
#include <vector>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/table/building_segment_reader.h"

DECLARE_REFERENCE_CLASS(index_base, InMemorySegment);
DECLARE_REFERENCE_CLASS(partition, InMemorySegmentContainer);
DECLARE_REFERENCE_CLASS(partition, CustomSegmentDumpItem);


IE_NAMESPACE_BEGIN(partition);

class DumpSegmentQueue
{
public:
    DumpSegmentQueue();
    ~DumpSegmentQueue();
    DumpSegmentQueue(const DumpSegmentQueue& other);
public:
    size_t Size() const
    {
        autil::ScopedLock lock(mLock);
        return mDumpItems.size();
    }
    void GetBuildingSegmentReaders(
        std::vector<table::BuildingSegmentReaderPtr>& buildingSegmentReaders,
        int64_t reclaimTimestamp = -1);
    DumpSegmentQueue* Clone();
    CustomSegmentDumpItemPtr GetLastSegment();
    void PushDumpItem(const CustomSegmentDumpItemPtr& dumpItem);
    // size_t GetDumpedQueueSize() const
    // {
    //     autil::ScopedLock lock(mLock);
    //     return mDumpedQueue.size();
    // };
    // size_t GetUnDumpedQueueSize() const
    // {
    //     autil::ScopedLock lock(mLock);
    //     return mDumpItems.size();
    // };
    void Clear()
    {
        autil::ScopedLock lock(mLock);
        mDumpItems.clear();
    }
    void GetDumpedItems(std::vector<CustomSegmentDumpItemPtr>& dumpedItems) const;
    void PopDumpedItems();
    bool ProcessOneItem(bool& hasNewDumpedSegment);
    uint64_t GetDumpingSegmentsSize() const;
    size_t GetEstimateDumpSize() const;
    void ReclaimSegments(int64_t reclaimTimestamp);

public:
    void LockQueue() { mLock.lock(); }
    void UnLockQueue() { mLock.unlock(); }
private:
    std::list<CustomSegmentDumpItemPtr> mDumpItems;
    mutable autil::RecursiveThreadMutex mLock;
    mutable autil::RecursiveThreadMutex mDumpLock;        
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DumpSegmentQueue);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_DUMP_SEGMENT_QUEUE_H
