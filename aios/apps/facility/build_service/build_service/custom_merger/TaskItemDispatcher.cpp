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
#include "build_service/custom_merger/TaskItemDispatcher.h"

#include <algorithm>
#include <cstddef>

using namespace std;

namespace build_service { namespace custom_merger {
BS_LOG_SETUP(custom_merger, TaskItemDispatcher);

TaskItemDispatcher::TaskItemDispatcher() {}

TaskItemDispatcher::~TaskItemDispatcher() {}

void TaskItemDispatcher::DispatchTasks(const std::vector<SegmentInfo>& segmentInfos,
                                       const std::vector<CustomMergerTaskItem>& taskItems, uint32_t totalInstanceCount,
                                       CustomMergeMeta& mergeMeta)
{
    vector<DispatchItem> items;
    for (size_t i = 0; i < segmentInfos.size(); i++) {
        for (size_t j = 0; j < taskItems.size(); j++) {
            DispatchItem item;
            item.segmentId = segmentInfos[i].segmentId;
            item.cost = segmentInfos[i].docCount * taskItems[j].cost;
            item.taskItemIdx = j;
            items.push_back(item);
        }
    }

    sort(items.begin(), items.end(),
         [](const DispatchItem& a, const DispatchItem& b) -> bool { return a.cost > b.cost; });
    size_t idx = 0;
    MergePlanHeap heap;
    for (size_t i = 0; i < totalInstanceCount; i++) {
        if (i >= items.size()) {
            break;
        }
        CustomMergePlanPtr mergePlan(new CustomMergePlan(i));
        int32_t taskIdx = items[i].taskItemIdx;
        mergePlan->addTaskItem(items[i].segmentId, taskItems[taskIdx], items[i].cost);
        heap.push(mergePlan);
        idx++;
    }
    for (; idx < items.size(); idx++) {
        CustomMergePlanPtr plan = heap.top();
        heap.pop();
        int32_t taskIdx = items[idx].taskItemIdx;
        plan->addTaskItem(items[idx].segmentId, taskItems[taskIdx], items[idx].cost);
        heap.push(plan);
    }
    while (!heap.empty()) {
        CustomMergePlanPtr plan = heap.top();
        mergeMeta.addMergePlan(*plan);
        heap.pop();
    }
}

}} // namespace build_service::custom_merger
