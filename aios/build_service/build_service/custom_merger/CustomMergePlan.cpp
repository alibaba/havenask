#include "build_service/custom_merger/CustomMergePlan.h"

using namespace std;

namespace build_service {
namespace custom_merger {
BS_LOG_SETUP(custom_merger, CustomMergePlan);

CustomMergePlan::CustomMergePlan(int32_t instanceId)
    : _instanceId(instanceId)
    , _cost(0.0)
{
}

CustomMergePlan::~CustomMergePlan() {
}

void CustomMergePlan::addTaskItem(int32_t segmentId, const CustomMergerTaskItem& taskItem, double cost) {
    string key = ToJsonString(taskItem);
    TaskItem& item = _taskItems[key];
    if (item.taskIdx == -1) {
        item.taskIdx = _taskCount;
        _taskCount++;
    }
    item.taskItem = taskItem;
    item.segments.push_back(segmentId);
    _cost += cost;
}

void CustomMergePlan::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
    vector<TaskItem> taskItems;
    if (json.GetMode() == TO_JSON) {
        std::map<std::string, TaskItem>::iterator iter = _taskItems.begin();
        for (; iter != _taskItems.end(); iter++)
        {
            taskItems.push_back(iter->second);
        }
    }
    json.Jsonize("task_items", taskItems, taskItems);
    if (json.GetMode() == FROM_JSON) {
        for (size_t i = 0; i < taskItems.size(); i++) {
            string key = ToJsonString(taskItems[i]);
            _taskItems[key] = taskItems[i];
        }
    }

    json.Jsonize("instance_id", _instanceId, _instanceId);
    json.Jsonize("cost", _cost, _cost);
    json.Jsonize("task_count", _taskCount, _taskCount);
}

void CustomMergePlan::getTaskItems(std::vector<TaskItem>& tasks) const
{
    for (auto &item: _taskItems) {
        tasks.push_back(item.second);
    }
}

}
}
