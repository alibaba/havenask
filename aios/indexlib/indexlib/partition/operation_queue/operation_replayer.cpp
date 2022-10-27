#include "indexlib/partition/operation_queue/operation_replayer.h"
#include "indexlib/partition/operation_queue/operation_iterator.h"
#include "indexlib/partition/operation_queue/operation_redo_strategy.h"
#include "indexlib/partition/operation_queue/operation_redo_hint.h"
#include "indexlib/index/online_join_policy.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/util/memory_control/simple_memory_quota_controller.h"
#include "indexlib/util/memory_control/build_resource_metrics.h"

using namespace std;

IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, OperationReplayer);

OperationReplayer::OperationReplayer(
    const index_base::PartitionDataPtr& partitionData,
    const config::IndexPartitionSchemaPtr& schema,
    const util::SimpleMemoryQuotaControllerPtr& memController)
    : mPartitionData(partitionData)
    , mSchema(schema)
    , mMemController(memController)
    , mRedoCount(0)
{
    
}

bool OperationReplayer::RedoOneOperation(
        const PartitionModifierPtr& modifier,
        OperationBase* operation,
        const OperationIterator& iter,
        const OperationRedoHint& redoHint)
{
    const util::BuildResourceMetricsPtr& buildResMetrics =
        modifier->GetBuildResourceMetrics();
    operation->Process(modifier, redoHint);
    ++mRedoCount;
    int64_t currentMemUse = buildResMetrics->GetValue(BMT_CURRENT_MEMORY_USE);
    int64_t currentUsedQuota = mMemController->GetUsedQuota();
    int64_t diff = 0;
    if (currentMemUse > currentUsedQuota)
    {
        diff = currentMemUse - currentUsedQuota;
        mMemController->Allocate(diff);
    }
    if (unlikely(mMemController->GetFreeQuota() <= 0))
    {
        IE_LOG(WARN, "redo operation fail, lack of memory, allocate[%ld], free quota [%ld]",
               diff, mMemController->GetFreeQuota());
        mCursor = iter.GetLastCursor();
        return false;
    }
    return true;
}

bool OperationReplayer::RedoOperations(
    const PartitionModifierPtr& modifier,
    const Version& onDiskVersion,
    const OperationRedoStrategyPtr& redoStrategy)
{
    IE_LOG(INFO, "redo operations begin");
    OnlineJoinPolicy joinPolicy(
        onDiskVersion, mSchema->GetTableType());
    
    int64_t reclaimTimestamp = joinPolicy.GetReclaimRtTimestamp();

    OperationIterator iter(mPartitionData, mSchema);
    iter.Init(reclaimTimestamp, mCursor);

    size_t skipRedoCount = 0;;
    OperationRedoHint redoHint;
    while (iter.HasNext())
    {
        OperationBase* operation = iter.Next();
        redoHint.Reset();
        if (redoStrategy && ! redoStrategy->NeedRedo(operation, redoHint))
        {
            skipRedoCount++;
            continue;
        }
        if (unlikely(!RedoOneOperation(modifier, operation, iter, redoHint)))
        {
            return false;
        }
    }
    mCursor = iter.GetLastCursor();

    IE_LOG(INFO, "redo operations end, total skip redo operation count: %lu",
           skipRedoCount);
    if (redoStrategy)
    {
        const OperationRedoCounter& redoCounter = redoStrategy->GetCounter();
        IE_LOG(INFO, "redo counter [%ld][%ld][%ld][%ld][%ld][%ld]",
               redoCounter.updateOpCount,
               redoCounter.deleteOpCount,
               redoCounter.otherOpCount,
               redoCounter.skipRedoUpdateOpCount,
               redoCounter.skipRedoDeleteOpCount,
               redoCounter.hintOpCount);
    }
    else
    {
        IE_LOG(WARN, "redoStrategy is null.");
    }
    IE_LOG(INFO, "redo operations end, total redo operation count: %lu",
           mRedoCount);
    return true;
}

IE_NAMESPACE_END(partition);

