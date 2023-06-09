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
#include "indexlib_plugin/plugins/aitheta/util/parallel_merge/parallel_merge_task.h"

using namespace std;
using namespace autil::legacy;

using namespace indexlib::index;
using namespace indexlib::index_base;

namespace indexlib {
namespace aitheta_plugin {
IE_LOG_SETUP(aitheta_plugin, ParallelMergeTask);

bool ParallelMergeTask::Create(uint32_t suggestTaskNum, const ParallelMergeInput &input,
                               vector<ParallelMergeTask> &parallelMergeTasks) {
    uint32_t taskNum = std::min(suggestTaskNum, (uint32_t)input.size());
    if (taskNum == 0u) {
        IE_LOG(WARN, "task num is 0, reset it to 1");
        taskNum = 1u;
    }

    parallelMergeTasks.clear();
    parallelMergeTasks.resize(taskNum);

    // mini heap, based on the document number assigned to the ParallelMergeTask
    auto cmp = [](const ParallelMergeTask *l, const ParallelMergeTask *r) { return l->docNum > r->docNum; };
    priority_queue<ParallelMergeTask *, vector<ParallelMergeTask *>, decltype(cmp)> heap(cmp);

    for (uint32_t id = 0u; id < (uint32_t)parallelMergeTasks.size(); ++id) {
        auto &task = parallelMergeTasks[id];
        task.taskId = id;
        heap.push(&task);
    }

    typedef std::pair<catid_t, size_t> Type;
    // sort documents based on the number in each category
    vector<Type> vec(input.begin(), input.end());
    sort(vec.begin(), vec.end(), [&](const Type &l, const Type &r) { return l.second > r.second; });

    // alway assign the current category to the ParallelMergeTask which has the smallest document number
    size_t totalNum = 0ul;
    for_each(vec.begin(), vec.end(), [&](const Type &t) {
        ParallelMergeTask *task = heap.top();
        heap.pop();
        totalNum += t.second;
        task->docNum += t.second;
        task->catidSet.insert(t.first);
        heap.push(task);
    });

    for_each(parallelMergeTasks.begin(), parallelMergeTasks.end(), [&](ParallelMergeTask &t) {
        if (totalNum != 0ul) {
            t.taskRatio = t.docNum / (float)totalNum;
        }
    });

    IE_LOG(INFO, "successfully create [%u] parallel merge tasks", taskNum);
    return true;
}

bool ParallelMergeTask::Transform(const std::vector<ParallelMergeTask> &parallelMergeTasks,
                                  MergeTaskResourceManagerPtr &resMgr,
                                  std::vector<indexlib::index::ReduceTask> &reduceTasks) {
    reduceTasks.clear();
    for (uint32_t id = 0u; id < parallelMergeTasks.size(); ++id) {
        const auto &task = parallelMergeTasks[id];
        ReduceTask reduceTask;
        reduceTask.id = task.taskId;
        reduceTask.dataRatio = task.taskRatio;
        try {
            string taskJson = autil::legacy::ToJsonString(task);
            reduceTask.resourceIds.push_back(resMgr->DeclareResource(taskJson.c_str(), taskJson.size()));
        } catch (const ExceptionBase &e) {
            IE_LOG(ERROR, "failed to jsonize the [%u] task, may deadly effect reduce work", id);
            return false;
        }
        reduceTasks.push_back(reduceTask);
    }
    return true;
}

// each reducer get one task
bool ParallelMergeTask::Retrieve(const MergeItemHint &mergeHint, const MergeTaskResourceVector &taskRscs,
                                 ParallelMergeTaskPtr &task) {
    if (mergeHint.GetId() == ParallelMergeItem::INVALID_PARALLEL_MERGE_ITEM_ID) {
        IE_LOG(WARN, "no task resources, disable parallel index build");
        return true;
    }

    if (taskRscs.size() != 1u) {
        IE_LOG(ERROR, "unexpected taskResources size[%lu]", taskRscs.size());
        return false;
    }

    task.reset(new ParallelMergeTask);

    const auto &taskRsc = taskRscs[0];
    string taskJson(taskRsc->GetData(), taskRsc->GetDataLen());
    try {
        autil::legacy::FromJsonString(task, taskJson);
    } catch (const ExceptionBase &e) {
        IE_LOG(ERROR, "failed to serialize task[%s]", taskJson.c_str());
        return false;
    }
    IE_LOG(INFO, "successfully retrieve task[%s]", taskJson.c_str());
    return true;
}

}
}
