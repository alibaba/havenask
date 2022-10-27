#include "indexlib/partition/open_executor/prejoin_executor.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/partition/partition_resource_calculator.h"
#include "indexlib/util/memory_control/block_memory_quota_controller.h"
#include "indexlib/partition/online_partition_metrics.h"

using namespace std;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(common);

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, PrejoinExecutor);

bool PrejoinExecutor::Execute(ExecutorResource& resource)
{
    misc::ScopeLatencyReporter scopeTime(
        resource.mOnlinePartMetrics.GetprejoinLatencyMetric().get());
    bool ret = mJoinSegWriter->PreJoin();
    mJoinSegWriter.reset();
    return ret;
}

IE_NAMESPACE_END(partition);

