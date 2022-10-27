#ifndef __INDEXLIB_COMMON_SEGMENT_SEGMENT_CONTAINER_H
#define __INDEXLIB_COMMON_SEGMENT_SEGMENT_CONTAINER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include <autil/WorkItem.h>

DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(common, DumpItem);

IE_NAMESPACE_BEGIN(index_base);

class SegmentContainer
{
public:
    SegmentContainer()
    {}
    
    virtual ~SegmentContainer() {}

public:
    virtual void CreateDumpItems(const file_system::DirectoryPtr& directory,
                                 std::vector<common::DumpItem*>& dumpItems) = 0;
    
    virtual size_t GetTotalMemoryUse() const = 0;
};

DEFINE_SHARED_PTR(SegmentContainer);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_COMMON_SEGMENT_SEGMENT_CONTAINER_H
