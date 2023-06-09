/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef __INDEXLIB_ONLINE_PARTITION_TASK_ITEM_H
#define __INDEXLIB_ONLINE_PARTITION_TASK_ITEM_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/TaskItem.h"

DECLARE_REFERENCE_CLASS(partition, OnlinePartition);

namespace indexlib { namespace partition {

class OnlinePartitionTaskItem : public util::TaskItem
{
public:
    enum TaskType {
        TT_REPORT_METRICS,
        TT_CLEAN_RESOURCE,
        TT_INTERVAL_DUMP,
        TT_TRIGGER_ASYNC_DUMP,
        TT_CHECK_SECONDARY_INDEX,
        TT_SUBSCRIBE_SECONDARY_INDEX,
        TT_SYNC_REALTIME_INDEX,
        TT_CALCULATE_METRICS,
        TT_UNKOWN
    };

public:
    OnlinePartitionTaskItem(OnlinePartition* onlinePartition, TaskType taskType)
        : mOnlinePartition(onlinePartition)
        , mTaskType(taskType)
    {
    }

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
}} // namespace indexlib::partition

#endif //__INDEXLIB_ONLINE_PARTITION_TASK_ITEM_H
