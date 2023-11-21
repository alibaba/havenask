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
#include "build_service/admin/controlflow/TaskSerializer.h"

#include <iosfwd>
#include <map>
#include <memory>
#include <string>

#include "alog/Logger.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "build_service/admin/controlflow/ControlDefine.h"
#include "build_service/admin/controlflow/TaskBase.h"
#include "build_service/admin/controlflow/TaskFactory.h"
#include "build_service/admin/controlflow/TaskResourceManager.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace build_service { namespace admin {
BS_LOG_SETUP(controlflow, TaskSerializer);

#undef LOG_PREFIX
#define LOG_PREFIX _resMgr->getLogPrefix().c_str()

TaskSerializer::TaskSerializer(const TaskFactoryPtr& factory, const TaskResourceManagerPtr& taskResMgr)
    : _factory(factory)
    , _resMgr(taskResMgr)
{
}

TaskSerializer::~TaskSerializer() {}

struct TaskSerializeItem : public Jsonizable {
public:
    TaskSerializeItem() {}
    TaskSerializeItem(const TaskBasePtr& task) : _task(task) {}

    void Jsonize(Jsonizable::JsonWrapper& json) override
    {
        TaskBase::TaskMetaPtr taskMeta = _task->createTaskMeta();
        json.Jsonize(TASK_META_STR, *taskMeta);
        json.Jsonize(TASK_INFO_STR, *_task);
    }

private:
    TaskBasePtr _task;
};

void TaskSerializer::serialize(Jsonizable::JsonWrapper& json, vector<TaskBasePtr>& tasks)
{
    vector<TaskSerializeItem> items;
    items.reserve(tasks.size());
    for (auto& task : tasks) {
        items.push_back(TaskSerializeItem(task));
    }
    json.Jsonize(TASK_ITEM_STR, items);
}

bool TaskSerializer::deserialize(Jsonizable::JsonWrapper& json, vector<TaskBasePtr>& tasks)
{
    JsonMap taskMap = json.GetMap();
    if (taskMap.find(TASK_ITEM_STR) == taskMap.end()) {
        BS_PREFIX_LOG(ERROR, "no %s find!", TASK_ITEM_STR.c_str());
        return false;
    }

    Any& any = taskMap[TASK_ITEM_STR];
    if (!IsJsonArray(any)) {
        return false;
    }

    tasks.clear();
    JsonArray array = AnyCast<JsonArray>(any);
    tasks.reserve(array.size());
    for (auto& flowAny : array) {
        JsonMap taskMap = AnyCast<JsonMap>(flowAny);
        if (taskMap.find(TASK_META_STR) == taskMap.end() || taskMap.find(TASK_INFO_STR) == taskMap.end()) {
            BS_PREFIX_LOG(ERROR, "invalid json structure!");
            return false;
        }

        TaskBase::TaskMeta meta;
        FromJson(meta, taskMap[TASK_META_STR]);
        TaskBasePtr task = _factory->createTaskObject(meta.getTaskId(), meta.getTaskType(), _resMgr);
        if (!task) {
            BS_PREFIX_LOG(ERROR, "create task object fail: taskId [%s], taskType [%s]", meta.getTaskId().c_str(),
                          meta.getTaskType().c_str());
            return false;
        }
        task->setPropertyMap(meta.getPropertyMap());
        task->setCycleTime(meta.getBeginTime(), meta.getEndTime());
        FromJson(*task, taskMap[TASK_INFO_STR]);
        tasks.push_back(task);
    }
    return true;
}

#undef LOG_PREFIX

}} // namespace build_service::admin
