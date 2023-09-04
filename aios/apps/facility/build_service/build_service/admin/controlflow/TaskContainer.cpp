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
#include "build_service/admin/controlflow/TaskContainer.h"

using namespace std;
using namespace autil;

namespace build_service { namespace admin {

BS_LOG_SETUP(admin, TaskContainer);

#define LOG_PREFIX _logPrefix.c_str()

TaskContainer::TaskContainer() {}

TaskContainer::~TaskContainer() {}

TaskBasePtr TaskContainer::getTask(const string& id)
{
    ScopedLock lock(_lock);
    TaskMap::const_iterator iter = _taskMap.find(id);
    if (iter != _taskMap.end()) {
        return iter->second;
    }
    return TaskBasePtr();
}

bool TaskContainer::addTask(const TaskBasePtr& task)
{
    if (!task) {
        return false;
    }
    ScopedLock lock(_lock);
    auto ret = _taskMap.insert(make_pair(task->getIdentifier(), task));
    if (!ret.second) {
        BS_PREFIX_LOG(ERROR, "duplicate task with same id [%s]", task->getIdentifier().c_str());
        return false;
    }

    BS_PREFIX_LOG(DEBUG, "add new task [id:%s]", task->getIdentifier().c_str());
    return true;
}

void TaskContainer::removeTask(const string& id)
{
    BS_PREFIX_LOG(INFO, "remove task [id:%s]", id.c_str());
    ScopedLock lock(_lock);
    _taskMap.erase(id);
}

vector<TaskBasePtr> TaskContainer::getAllTask() const
{
    ScopedLock lock(_lock);
    vector<TaskBasePtr> ret;
    ret.reserve(_taskMap.size());

    TaskMap::const_iterator iter = _taskMap.begin();
    for (; iter != _taskMap.end(); iter++) {
        ret.push_back(iter->second);
    }
    return ret;
}

void TaskContainer::reset()
{
    ScopedLock lock(_lock);
    _taskMap.clear();
}

#undef LOG_PREFIX

}} // namespace build_service::admin
