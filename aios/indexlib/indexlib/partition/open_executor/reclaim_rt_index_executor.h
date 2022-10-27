#ifndef __INDEXLIB_RECLAIM_RT_INDEX_EXECUTOR_H
#define __INDEXLIB_RECLAIM_RT_INDEX_EXECUTOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/open_executor/open_executor.h"
#include "indexlib/partition/realtime_partition_data_reclaimer.h"

IE_NAMESPACE_BEGIN(partition);

class ReclaimRtIndexExecutor : public OpenExecutor
{
public:
    ReclaimRtIndexExecutor()
    { }
    ~ReclaimRtIndexExecutor()
    { }

    bool Execute(ExecutorResource& resource) override;
    void Drop(ExecutorResource& resource) override;
private:
    index_base::PartitionDataPtr mOriginalPartitionData;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ReclaimRtIndexExecutor);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_RECLAIM_RT_INDEX_EXECUTOR_H
