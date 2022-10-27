#ifndef __INDEXLIB_RECLAIM_RT_SEGMENTS_EXECUTOR_H
#define __INDEXLIB_RECLAIM_RT_SEGMENTS_EXECUTOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/open_executor/open_executor.h"
#include "indexlib/partition/realtime_partition_data_reclaimer.h"

IE_NAMESPACE_BEGIN(partition);

class ReclaimRtSegmentsExecutor : public OpenExecutor
{
public:
    ReclaimRtSegmentsExecutor(bool needReclaimBuildingSegment = true);
    ~ReclaimRtSegmentsExecutor();
public:
    bool Execute(ExecutorResource& resource) override;
    void Drop(ExecutorResource& resource) override;
private:
    index_base::PartitionDataPtr mOriginalPartitionData;
    bool mNeedReclaimBuildingSegment;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ReclaimRtSegmentsExecutor);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_RECLAIM_RT_SEGMENTS_EXECUTOR_H
