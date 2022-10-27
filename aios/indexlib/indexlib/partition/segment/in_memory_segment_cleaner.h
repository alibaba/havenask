#ifndef __INDEXLIB_IN_MEMORY_SEGMENT_CLEANER_H
#define __INDEXLIB_IN_MEMORY_SEGMENT_CLEANER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/executor.h"

DECLARE_REFERENCE_CLASS(partition, InMemorySegmentContainer);

IE_NAMESPACE_BEGIN(partition);

class InMemorySegmentCleaner : public common::Executor
{
public:
    InMemorySegmentCleaner(const InMemorySegmentContainerPtr& container);
    ~InMemorySegmentCleaner();

public:
    void Execute() override;

private:
    InMemorySegmentContainerPtr mInMemSegContainer;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(InMemorySegmentCleaner);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_IN_MEMORY_SEGMENT_CLEANER_H
