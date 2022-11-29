#include "aitheta_indexer/plugins/aitheta/util/reduce_task/reduce_task_creater.h"

using namespace std;
using namespace autil::legacy;

IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(aitheta_plugin);
IE_LOG_SETUP(aitheta_plugin, ReduceTaskCreater);

vector<ReduceTask> ReduceTaskCreater::Create(uint32_t suggestTaskNum, bool isEntireDataSet,
                                             MergeTaskResourceManagerPtr &resMgr, bool hasIndexSeg,
                                             const ReduceTaskInput &taskInput) {
    vector<ReduceTask> reduceTasks;
    uint32_t taskNum = suggestTaskNum;
    if (0u == taskNum) {
        throw ExceptionBase("invalid task num");
    }

    if (!isEntireDataSet && !hasIndexSeg) {
        // disable parallel merge when inc merge
        taskNum = 1u;
    }

    vector<CustomReduceTask> custTasks;
    if (hasIndexSeg) {
        for (uint32_t id = 0; id < taskNum; ++id) {
            custTasks.emplace_back(id, 0u, 1.0f / taskNum, std::set<int64_t>({}));
        }
    } else {
        custTasks = CustomCreate(taskInput, taskNum);
    }

    for (uint32_t id = 0; id < custTasks.size(); ++id) {
        const auto &customTask = custTasks[id];
        ReduceTask reduceTask;
        reduceTask.id = customTask.taskId;
        reduceTask.dataRatio = customTask.taskRatio;

        string taskJson;
        try {
            taskJson = autil::legacy::ToJsonString(customTask);
        } catch (const exception &e) {
            INDEXLIB_FATAL_ERROR(InconsistentState,
                                 "failed to jsonize the [%u] task out of [%u], may deadly effect reduce work", id,
                                 taskNum);
            return {};
        }
        reduceTask.resourceIds.push_back(resMgr->DeclareResource(taskJson.c_str(), taskJson.size()));
        reduceTasks.push_back(reduceTask);
    }

    IE_LOG(INFO, "successfully create [%u] reduce tasks", taskNum);
    return reduceTasks;
}

// each reducer get one task
bool ReduceTaskCreater::Retrieve(const MergeItemHint &mergeHint, const MergeTaskResourceVector &taskResources,
                                 CustomReduceTaskPtr &task) {
    if (mergeHint.GetId() == ParallelMergeItem::INVALID_PARALLEL_MERGE_ITEM_ID) {
        IE_LOG(WARN, "no task resources, disable parallel index build");
        return true;
    }

    if (taskResources.empty()) {
        IE_LOG(ERROR, "no task resources");
        return false;
    }

    task.reset(new CustomReduceTask);

    if (taskResources.size() != 1u) {
        IE_LOG(ERROR, "unexpected taskResources size[%lu]", taskResources.size());
        return false;
    }
    const auto &taskResource = taskResources[0];
    string taskJson(taskResource->GetData(), taskResource->GetDataLen());
    try {
        autil::legacy::FromJsonString(task, taskJson);
    } catch (const exception &e) {
        IE_LOG(ERROR, "failed to serialize task[%s]", taskJson.c_str());
        return false;
    }
    IE_LOG(INFO, "successfully retrieve task[%s]", taskJson.c_str());
    return true;
}

vector<CustomReduceTask> ReduceTaskCreater::CustomCreate(const ReduceTaskInput &taskInput, uint32_t taskNum) {
    vector<CustomReduceTask> reduceTasks(taskNum);

    // mini heap, based on the document number assigned to the CustomReduceTask
    auto compare = [](const CustomReduceTask *lhs, const CustomReduceTask *rhs) {
        return lhs->documentCnt > rhs->documentCnt;
    };
    priority_queue<CustomReduceTask *, vector<CustomReduceTask *>, decltype(compare)> heap(compare);

    for (uint32_t id = 0; id < (uint32_t)reduceTasks.size(); ++id) {
        auto &reduceTask = reduceTasks[id];
        reduceTask.taskId = id;
        heap.push(&reduceTask);
    }

    // sort documents based on the number in each category
    vector<pair<int64_t, size_t>> categories(taskInput.begin(), taskInput.end());
    sort(categories.begin(), categories.end(),
         [](const pair<int64_t, size_t> &lhs, const pair<int64_t, size_t> &rhs) { return lhs.second > rhs.second; });

    // alway assign the current category to the CustomReduceTask which has the smallest document number
    size_t totalCnt = 0u;
    for (const auto &kv : categories) {
        auto reduceTask = heap.top();
        heap.pop();
        reduceTask->documentCnt += kv.second;
        totalCnt += kv.second;
        reduceTask->categorySet.insert(kv.first);
        heap.push(reduceTask);
    }

    for (auto &task : reduceTasks) {
        if (totalCnt == 0u) {
            task.taskRatio = 1.0f / taskNum;
        } else {
            task.taskRatio = task.documentCnt / (float)totalCnt;
        }
    }

    return reduceTasks;
}

IE_NAMESPACE_END(aitheta_plugin);
