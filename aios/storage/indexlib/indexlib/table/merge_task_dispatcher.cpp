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
#include "indexlib/table/merge_task_dispatcher.h"

#include <algorithm>
#include <cstddef>
#include <ext/alloc_traits.h>
#include <queue>

using namespace std;

namespace indexlib { namespace table {
IE_LOG_SETUP(table, MergeTaskDispatcher);

MergeTaskDispatcher::MergeTaskDispatcher() {}

MergeTaskDispatcher::~MergeTaskDispatcher() {}

vector<TaskExecuteMetas> MergeTaskDispatcher::DispatchMergeTasks(const vector<MergeTaskDescriptions>& taskDescriptions,
                                                                 uint32_t instanceCount)
{
    std::vector<TaskExecuteMetas> taskExecMetas;
    if (taskDescriptions.size() == 0) {
        return taskExecMetas;
    }
    taskExecMetas.reserve(instanceCount);
    for (size_t i = 0; i < instanceCount; ++i) {
        TaskExecuteMetas taskMetas;
        taskExecMetas.push_back(taskMetas);
    }

    TaskExecuteMetas allTaskMetas;
    for (size_t planIdx = 0; planIdx < taskDescriptions.size(); ++planIdx) {
        for (size_t taskIdx = 0; taskIdx < taskDescriptions[planIdx].size(); ++taskIdx) {
            allTaskMetas.push_back(TaskExecuteMeta(planIdx, taskIdx, taskDescriptions[planIdx][taskIdx].cost));
        }
    }

    // sort allTaskMetas by cost in descending order
    auto CompByCost = [](const TaskExecuteMeta& lhs, const TaskExecuteMeta& rhs) { return lhs.cost > rhs.cost; };
    sort(allTaskMetas.begin(), allTaskMetas.end(), CompByCost);

    // make a min-heap, WorkloadCounter with lowest workLoad on top
    priority_queue<WorkLoadCounterPtr, std::vector<WorkLoadCounterPtr>, WorkLoadComp> totalLoadHeap;
    for (size_t i = 0; i < instanceCount; ++i) {
        WorkLoadCounterPtr counter(new WorkLoadCounter(taskExecMetas[i], 0));
        totalLoadHeap.push(counter);
    }

    // iterate all taskMetas, and assign task with largest cost
    // to the WorkLoadCounter with lowest total load
    for (const auto& taskMeta : allTaskMetas) {
        WorkLoadCounterPtr counterOnTop = totalLoadHeap.top();
        totalLoadHeap.pop();
        counterOnTop->taskMetas.push_back(taskMeta);
        counterOnTop->workLoad += taskMeta.cost;
        totalLoadHeap.push(counterOnTop);
    }
    return taskExecMetas;
}
}} // namespace indexlib::table
