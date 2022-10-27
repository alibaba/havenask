#ifndef __INDEXLIB_ONLINE_PARTITION_TASK_ITEM_H
#define __INDEXLIB_ONLINE_PARTITION_TASK_ITEM_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/util/task_item.h"

DECLARE_REFERENCE_CLASS(partition, OnlinePartition);

IE_NAMESPACE_BEGIN(partition);

class OnlinePartitionTaskItem : public util::TaskItem
{
public:
    enum TaskType
    {
        TT_REPORT_METRICS,
        TT_CLEAN_RESOURCE,
        TT_INTERVAL_DUMP,
        TT_TRIGGER_ASYNC_DUMP,
        TT_CHECK_SECONDARY_INDEX,
        TT_SUBSCRIBE_SECONDARY_INDEX,
        TT_UNKOWN
    };
    
public:
    OnlinePartitionTaskItem(OnlinePartition* onlinePartition, TaskType taskType)
        : mOnlinePartition(onlinePartition)
        , mTaskType(taskType)
    {}
    
    ~OnlinePartitionTaskItem() {}
    
public:
    void Run() override;
    
private:
    OnlinePartition* mOnlinePartition;
    TaskType mTaskType;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OnlinePartitionTaskItem);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_ONLINE_PARTITION_TASK_ITEM_H
