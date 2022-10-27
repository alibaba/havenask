#ifndef __INDEXLIB_GENERATE_JOIN_SEGMENT_EXECUTOR_H
#define __INDEXLIB_GENERATE_JOIN_SEGMENT_EXECUTOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/online_partition_metrics.h"
#include "indexlib/partition/open_executor/open_executor.h"
#include "indexlib/partition/open_executor/executor_resource.h"

DECLARE_REFERENCE_CLASS(partition, JoinSegmentWriter);

IE_NAMESPACE_BEGIN(partition);

class GenerateJoinSegmentExecutor : public OpenExecutor
{
public:
    GenerateJoinSegmentExecutor(const JoinSegmentWriterPtr& joinSegWriter)
        : mJoinSegWriter(joinSegWriter)
    { }
    ~GenerateJoinSegmentExecutor() { }

    bool Execute(ExecutorResource& resource) override;
    void Drop(ExecutorResource& resource) override;

private:
    JoinSegmentWriterPtr mJoinSegWriter;
    index_base::PartitionDataPtr mOriginalPartitionData;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(GenerateJoinSegmentExecutor);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_GENERATE_JOIN_SEGMENT_EXECUTOR_H
