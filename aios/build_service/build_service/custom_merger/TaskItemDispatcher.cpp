#include "build_service/custom_merger/TaskItemDispatcher.h"

using namespace std;

namespace build_service {
namespace custom_merger {
BS_LOG_SETUP(custom_merger, TaskItemDispatcher);

TaskItemDispatcher::TaskItemDispatcher() {
}

TaskItemDispatcher::~TaskItemDispatcher() {
}

void TaskItemDispatcher::DispatchTasks(const std::vector<SegmentInfo>& segmentInfos,
                                       const std::vector<CustomMergerTaskItem>& taskItems,
                                       uint32_t totalInstanceCount,
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

    sort(items.begin(), items.end(), [](const DispatchItem& a, const DispatchItem& b) -> bool {
                return a.cost > b.cost;
            });
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
    while(!heap.empty()) {
        CustomMergePlanPtr plan = heap.top();
        mergeMeta.addMergePlan(*plan);
        heap.pop();
    }
}

}
}
