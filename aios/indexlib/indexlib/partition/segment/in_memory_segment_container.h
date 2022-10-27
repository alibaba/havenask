#ifndef __INDEXLIB_IN_MEMORY_SEGMENT_CONTAINER_H
#define __INDEXLIB_IN_MEMORY_SEGMENT_CONTAINER_H

#include <tr1/memory>
#include <autil/Lock.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(index_base, InMemorySegment);
IE_NAMESPACE_BEGIN(partition);

class InMemorySegmentContainer
{
public:
    InMemorySegmentContainer();
    ~InMemorySegmentContainer();
public:
    void PushBack(const index_base::InMemorySegmentPtr& inMemSeg);
    void EvictOldestInMemSegment();
    index_base::InMemorySegmentPtr GetOldestInMemSegment() const;
    void WaitEmpty() const;

    size_t GetTotalMemoryUse() const;

    size_t Size() const
    {
        autil::ScopedLock lock(mInMemSegVecLock);
        return mInMemSegVec.size();
    }

private:
    std::vector<index_base::InMemorySegmentPtr> mInMemSegVec;
    mutable autil::ThreadCond mInMemSegVecLock;
    mutable volatile size_t mTotalMemUseCache;

    static const size_t INVALID_MEMORY_USE;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(InMemorySegmentContainer);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_IN_MEMORY_SEGMENT_CONTAINER_H
