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
#include "build_service/custom_merger/CustomMergePlan.h"

#include <cstddef>
#include <ext/alloc_traits.h>
#include <utility>

#include "autil/legacy/jsonizable.h"
#include "autil/legacy/legacy_jsonizable.h"

using namespace std;

namespace build_service { namespace custom_merger {
BS_LOG_SETUP(custom_merger, CustomMergePlan);

CustomMergePlan::CustomMergePlan(int32_t instanceId) : _instanceId(instanceId), _cost(0.0) {}

CustomMergePlan::~CustomMergePlan() {}

void CustomMergePlan::addTaskItem(int32_t segmentId, const CustomMergerTaskItem& taskItem, double cost)
{
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

void CustomMergePlan::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    vector<TaskItem> taskItems;
    if (json.GetMode() == TO_JSON) {
        std::map<std::string, TaskItem>::iterator iter = _taskItems.begin();
        for (; iter != _taskItems.end(); iter++) {
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
    for (auto& item : _taskItems) {
        tasks.push_back(item.second);
    }
}

}} // namespace build_service::custom_merger
