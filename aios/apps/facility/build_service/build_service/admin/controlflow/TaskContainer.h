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
#ifndef ISEARCH_BS_CONTROL_TASK_CONTAINER_H
#define ISEARCH_BS_CONTROL_TASK_CONTAINER_H

#include <map>
#include <memory>

#include "autil/Lock.h"
#include "build_service/admin/controlflow/TaskBase.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

class TaskContainer
{
public:
    TaskContainer();
    ~TaskContainer();

public:
    TaskBasePtr getTask(const std::string& id);
    bool addTask(const TaskBasePtr& task);
    void removeTask(const std::string& id);
    std::vector<TaskBasePtr> getAllTask() const;

    void reset();

    void setLogPrefix(const std::string& str) { _logPrefix = str; }
    const std::string& getLogPrefix() const { return _logPrefix; }

private:
    typedef std::map<std::string, TaskBasePtr> TaskMap;
    TaskMap _taskMap;
    mutable autil::RecursiveThreadMutex _lock;

    std::string _logPrefix;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(TaskContainer);

}} // namespace build_service::admin

#endif // ISEARCH_BS_CONTROL_TASK_CONTAINER_H
