#include "indexlib/partition/online_partition_task_item.h"
#include "indexlib/partition/online_partition.h"

using namespace std;

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, OnlinePartitionTaskItem);

void OnlinePartitionTaskItem::Run()
{
    mOnlinePartition->ExecuteTask(mTaskType);
}

IE_NAMESPACE_END(partition);

