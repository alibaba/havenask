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
#ifndef ISEARCH_BS_CONTROL_FLOW_TASK_WRAPPER_H
#define ISEARCH_BS_CONTROL_FLOW_TASK_WRAPPER_H

#include <memory>

#include "build_service/admin/controlflow/Eluna.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

BS_DECLARE_REFERENCE_CLASS(admin, TaskBase);

namespace build_service { namespace admin {

class TaskWrapper
{
public:
    TaskWrapper();
    ~TaskWrapper();

public:
    // for lua
    const char* getIdentifier();
    const char* getType();
    bool isValid();

    const char* getProperty(const char* key);
    bool setProperty(const char* key, const char* value);

    bool start(const char* kvParamStr);
    bool finish(const char* kvParamStr);
    bool executeCmd(const char* cmdName, const char* kvParamStr);
    int64_t getEndTime();

    // bool stop();
    bool isTaskFinishing();
    bool isTaskFinish();
    bool isTaskStopping();
    bool isTaskStopped();
    bool isTaskRunning();
    bool isTaskError();

public:
    const TaskBasePtr& getTask() const { return _taskPtr; }
    void setTask(const TaskBasePtr& task) { _taskPtr = task; }

    static void bindLua(lua_State* state);

private:
    TaskBasePtr _taskPtr;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(TaskWrapper);

}} // namespace build_service::admin

#endif // ISEARCH_BS_CONTROL_FLOW_TASK_WRAPPER_H
