#ifndef __INDEXLIB_IN_MEMORY_SEGMENT_RELEASER_H
#define __INDEXLIB_IN_MEMORY_SEGMENT_RELEASER_H

#include <tr1/memory>
#include <autil/WorkItem.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(index_base, InMemorySegment);

IE_NAMESPACE_BEGIN(partition);

class InMemorySegmentReleaser : public autil::WorkItem
{
public:
    InMemorySegmentReleaser(const index_base::InMemorySegmentPtr& inMemorySegment);
    ~InMemorySegmentReleaser();

public:
    void process() override;
    void destroy() override;
    void drop() override;

private:
    index_base::InMemorySegmentPtr mInMemorySegment;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(InMemorySegmentReleaser);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_IN_MEMORY_SEGMENT_RELEASER_H
