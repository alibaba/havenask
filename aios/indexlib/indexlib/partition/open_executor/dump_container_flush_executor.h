#ifndef __INDEXLIB_DUMP_CONTAINER_FLUSH_EXECUTOR_H
#define __INDEXLIB_DUMP_CONTAINER_FLUSH_EXECUTOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/open_executor/open_executor.h"
#include "indexlib/partition/segment/dump_segment_container.h"

IE_NAMESPACE_BEGIN(partition);

class DumpContainerFlushExecutor : public OpenExecutor
{
public:
    DumpContainerFlushExecutor(autil::RecursiveThreadMutex* dataLock, const std::string& partitionName = "");
    ~DumpContainerFlushExecutor();
public:
    bool Execute(ExecutorResource& resource) override;
    void Drop(ExecutorResource& resource) override {}
private:
    void ReopenNewSegment(ExecutorResource& resource);
private:
    index_base::PartitionDataPtr mOriginalPartitionData;
    DumpSegmentContainerPtr mOriginalDumpSegmentContainer;
    autil::RecursiveThreadMutex* mDataLock;
    
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DumpContainerFlushExecutor);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_DUMP_CONTAINER_FLUSH_EXECUTOR_H
