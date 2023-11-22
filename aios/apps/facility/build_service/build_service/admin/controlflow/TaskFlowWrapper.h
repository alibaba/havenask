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
#pragma once

#include "build_service/admin/controlflow/TaskWrapper.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

BS_DECLARE_REFERENCE_CLASS(admin, TaskFlow);

extern "C" {
#include "lua.h"
}

#include "build_service/admin/controlflow/TaskWrapper.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

BS_DECLARE_REFERENCE_CLASS(admin, TaskFlow);

namespace build_service { namespace admin {

class TaskFlowWrapper
{
public:
    TaskFlowWrapper();
    ~TaskFlowWrapper();

public:
    bool isValid();

    void setGlobalTaskParam(const char* key, const char* value);
    TaskWrapper createTask(const char* taskId, const char* kernalType, const char* kvParamStr);
    TaskWrapper getTask(const char* taskId);
    bool finishTask(const char* taskId, const char* params);
    TaskWrapper getTaskInFlow(const char* taskId, const char* flowId);

    // waitStatus : "xxx|xxx|xxx", xxx=eof or finish or stop or error or init or running
    // waitStatus = "", will default set to finish
    void addUpstreamFlowById(const char* flowId, const char* waitStatus, const char* missingAction);
    void addUpstreamFlow(TaskFlowWrapper& wrapper, const char* waitStatus, const char* missingAction);
    void addFriendFlowId(const char* flowId);
    void addFriendFlow(TaskFlowWrapper& wrapper);
    void setPublic(bool isPublic);
    bool suspendFlow();
    bool resumeFlow();
    bool stopFlow();

    bool isFlowRunning();
    bool isFlowStopped();
    bool isFlowFinish();
    bool isFlowSuspended();
    bool hasRuntimeError();
    bool hasFatalError();
    void addTag(const char* tag);
    const char* getFlowId();
    bool openApi(const char* cmd, const char* path, const char* params);
    // attention: fatal = true, will stop current flow
    void setError(const char* errMsg, bool fatal);

    const char* getProperty(const char* key);
    const char* getFlowProperty(const char* flowId, const char* key);
    const char* getFlowStatus(const char* flowId);
    void setProperty(const char* key, const char* value);

public:
    TaskFlow* getTaskFlow() const { return _taskFlow; }
    void setTaskFlow(TaskFlow* taskFlow) { _taskFlow = taskFlow; }
    TaskWrapper createTaskWrapperWithKvParam(const char* taskId, const char* kernalType, const KeyValueMap& kvMap);

    static void bindLua(lua_State* state);

private:
    TaskFlow* _taskFlow;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(TaskFlowWrapper);

}} // namespace build_service::admin
