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
#ifndef ISEARCH_BS_TASKITEMDISPATCHER_H
#define ISEARCH_BS_TASKITEMDISPATCHER_H

#include <queue>

#include "build_service/common_define.h"
#include "build_service/custom_merger/CustomMergeMeta.h"
#include "build_service/custom_merger/CustomMerger.h"
#include "build_service/util/Log.h"
#include "indexlib/config/index_partition_schema.h"

namespace build_service { namespace custom_merger {

class TaskItemDispatcher
{
public:
    struct SegmentInfo {
        int32_t segmentId;
        size_t docCount;
    };

private:
    struct DispatchItem {
        double cost;
        int32_t segmentId;
        CustomMergerTaskItem taskItem;
        int32_t taskItemIdx;
    };
    class MergePlanComparator
    {
    public:
        bool operator()(const CustomMergePlanPtr& left, const CustomMergePlanPtr& right)
        {
            return left->getCost() > right->getCost();
        }
    };
    typedef std::priority_queue<CustomMergePlanPtr, std::vector<CustomMergePlanPtr>, MergePlanComparator> MergePlanHeap;

public:
    TaskItemDispatcher();
    ~TaskItemDispatcher();

public:
    static void DispatchTasks(const std::vector<SegmentInfo>& segmentInfos,
                              const std::vector<CustomMergerTaskItem>& taskItems, uint32_t totalInstanceCount,
                              CustomMergeMeta& mergeMeta);

private:
    TaskItemDispatcher(const TaskItemDispatcher&);
    TaskItemDispatcher& operator=(const TaskItemDispatcher&);

public:
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(TaskItemDispatcher);

}} // namespace build_service::custom_merger

#endif // ISEARCH_BS_TASKITEMDISPATCHER_H
