#ifndef __INDEXLIB_DUMP_SEGMENT_CONTAINER_H
#define __INDEXLIB_DUMP_SEGMENT_CONTAINER_H

#include <tr1/memory>
#include <autil/Lock.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(index_base, InMemorySegment);
DECLARE_REFERENCE_CLASS(partition, InMemorySegmentContainer);
DECLARE_REFERENCE_CLASS(partition, NormalSegmentDumpItem);
DECLARE_REFERENCE_CLASS(partition, DumpSegmentContainer);

IE_NAMESPACE_BEGIN(partition);

class DumpSegmentContainer
{
public:
    DumpSegmentContainer();
    virtual ~DumpSegmentContainer();
    DumpSegmentContainer(const DumpSegmentContainer& other);
public:
    void PushDumpItem(const NormalSegmentDumpItemPtr& dumpItem);
    InMemorySegmentContainerPtr GetInMemSegmentContainer() { return mInMemSegContainer; }
    void GetDumpingSegments(std::vector<index_base::InMemorySegmentPtr>& dumpingSegments);
    void GetSubDumpingSegments(std::vector<index_base::InMemorySegmentPtr>& dumpingSegments);
    void GetDumpedSegments(std::vector<index_base::InMemorySegmentPtr>& dumpItems);
    uint64_t GetMaxDumpingSegmentExpandMemUse();
    index_base::InMemorySegmentPtr GetLastSegment();
    DumpSegmentContainer* Clone();
    virtual void ClearDumpedSegment();
    size_t GetDumpItemSize()
    {
        autil::ScopedLock lock(mLock);
        return mDumpItems.size();
    }
    uint64_t GetDumpingSegmentsSize();
    size_t GetUnDumpedSegmentSize();
    void TrimObsoleteDumpingSegment(int64_t ts);
    virtual NormalSegmentDumpItemPtr GetOneSegmentItemToDump();
private:
    std::vector<NormalSegmentDumpItemPtr> mDumpItems;
    InMemorySegmentContainerPtr mInMemSegContainer;
    mutable autil::RecursiveThreadMutex mLock;
private:
    IE_LOG_DECLARE();
};

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_DUMP_SEGMENT_CONTAINER_H
