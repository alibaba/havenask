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
#include "build_service/admin/controlflow/TaskFlowWrapper.h"

#include "autil/legacy/exception.h"
#include "autil/legacy/jsonizable.h"
#include "build_service/admin/controlflow/ControlDefine.h"
#include "build_service/admin/controlflow/KeyValueParamParser.h"
#include "build_service/admin/controlflow/TaskFlow.h"

using namespace std;
using namespace autil::legacy;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, TaskFlowWrapper);

#define LOG_PREFIX _taskFlow->_logPrefix.c_str()

TaskFlowWrapper::TaskFlowWrapper() : _taskFlow(NULL) {}

TaskFlowWrapper::~TaskFlowWrapper() {}

void TaskFlowWrapper::setGlobalTaskParam(const char* key, const char* value)
{
    if (_taskFlow) {
        _taskFlow->setGlobalTaskParam(key, value);
    }
}

bool TaskFlowWrapper::isValid() { return _taskFlow != NULL; }

bool TaskFlowWrapper::finishTask(const char* taskId, const char* params)
{
    auto taskWrapper = getTask(taskId);
    return taskWrapper.finish(params);
}

TaskWrapper TaskFlowWrapper::getTask(const char* taskId)
{
    TaskWrapper ret;
    if (_taskFlow) {
        ret.setTask(_taskFlow->getTask(taskId));
    }
    return ret;
}

void TaskFlowWrapper::addTag(const char* tag)
{
    if (_taskFlow) {
        _taskFlow->addTag(tag);
    }
}

bool TaskFlowWrapper::openApi(const char* cmd, const char* path, const char* params)
{
    KeyValueMap kvMap;
    if (!KeyValueParamParser::parseFromJsonString(params, kvMap)) {
        return false;
    }
    if (_taskFlow) {
        return _taskFlow->openApi(cmd, path, kvMap);
    }
    return false;
}

TaskWrapper TaskFlowWrapper::getTaskInFlow(const char* taskId, const char* flowId)
{
    TaskWrapper ret;
    if (_taskFlow) {
        ret.setTask(_taskFlow->getTaskInFlow(taskId, flowId));
    }
    return ret;
}

void TaskFlowWrapper::addUpstreamFlowById(const char* flowId, const char* waitStatus, const char* missingAction)
{
    if (!flowId) {
        return;
    }

    string actionStr = !missingAction ? "stop" : string(missingAction);
    if (_taskFlow) {
        if (!waitStatus) {
            _taskFlow->addUpstreamFlowId(flowId, "finish", actionStr);
        } else {
            _taskFlow->addUpstreamFlowId(flowId, waitStatus, actionStr);
        }
    }
}

void TaskFlowWrapper::setPublic(bool isPublic)
{
    if (_taskFlow) {
        _taskFlow->setPublic(isPublic);
    }
}

void TaskFlowWrapper::addUpstreamFlow(TaskFlowWrapper& wrapper, const char* waitStatus, const char* missingAction)
{
    if (!wrapper.isValid() || !isValid()) {
        return;
    }
    addUpstreamFlowById(wrapper.getFlowId(), waitStatus, missingAction);
}

void TaskFlowWrapper::addFriendFlowId(const char* flowId)
{
    if (!flowId) {
        return;
    }
    _taskFlow->addFriendFlowId(flowId);
}

void TaskFlowWrapper::addFriendFlow(TaskFlowWrapper& wrapper)
{
    if (!wrapper.isValid() || !isValid()) {
        return;
    }
    addFriendFlowId(wrapper.getFlowId());
}

const char* TaskFlowWrapper::getFlowId()
{
    if (!isValid()) {
        return NULL;
    }
    return _taskFlow->getFlowId().c_str();
}

bool TaskFlowWrapper::isFlowRunning()
{
    if (_taskFlow) {
        return _taskFlow->isFlowRunning();
    }
    return false;
}

bool TaskFlowWrapper::isFlowStopped()
{
    if (_taskFlow) {
        return _taskFlow->isFlowStopped();
    }
    return false;
}

bool TaskFlowWrapper::isFlowSuspended()
{
    if (_taskFlow) {
        return _taskFlow->isFlowSuspended();
    }
    return false;
}

bool TaskFlowWrapper::hasRuntimeError()
{
    if (_taskFlow) {
        return _taskFlow->hasRuntimeError();
    }
    return false;
}

bool TaskFlowWrapper::hasFatalError()
{
    if (_taskFlow) {
        return _taskFlow->hasFatalError();
    }
    return false;
}

void TaskFlowWrapper::setError(const char* errMsg, bool fatal)
{
    if (_taskFlow) {
        _taskFlow->setError(errMsg, fatal);
    }
}

bool TaskFlowWrapper::isFlowFinish()
{
    if (_taskFlow) {
        return _taskFlow->isFlowFinish();
    }
    return false;
}

bool TaskFlowWrapper::suspendFlow()
{
    if (_taskFlow) {
        return _taskFlow->suspendFlow();
    }
    return false;
}

bool TaskFlowWrapper::resumeFlow()
{
    if (_taskFlow) {
        return _taskFlow->resumeFlow();
    }
    return false;
}

bool TaskFlowWrapper::stopFlow()
{
    if (_taskFlow) {
        return _taskFlow->stopFlow();
    }
    return false;
}

TaskWrapper TaskFlowWrapper::createTask(const char* taskId, const char* kernalType, const char* kvParamStr)
{
    KeyValueMap kvMap;
    if (!KeyValueParamParser::parseFromJsonString(kvParamStr, kvMap)) {
        return TaskWrapper();
    }
    return createTaskWrapperWithKvParam(taskId, kernalType, kvMap);
}

TaskWrapper TaskFlowWrapper::createTaskWrapperWithKvParam(const char* taskId, const char* kernalType,
                                                          const KeyValueMap& kvMap)
{
    TaskWrapper taskWrapper;
    if (!_taskFlow) {
        return taskWrapper;
    }

    TaskBasePtr task = _taskFlow->createTask(taskId, kernalType, kvMap);
    if (!task) {
        BS_PREFIX_LOG(ERROR, "create task [%s] fail!", taskId);
        return taskWrapper;
    }
    taskWrapper.setTask(task);
    return taskWrapper;
}

const char* TaskFlowWrapper::getProperty(const char* key)
{
    if (!_taskFlow) {
        return "";
    }
    return _taskFlow->getProperty(key).c_str();
}

const char* TaskFlowWrapper::getFlowProperty(const char* flowId, const char* key)
{
    if (!_taskFlow) {
        return "";
    }
    return _taskFlow->getFlowProperty(flowId, key).c_str();
}

const char* TaskFlowWrapper::getFlowStatus(const char* flowId)
{
    if (!_taskFlow) {
        return "";
    }
    return _taskFlow->getFlowStatus(flowId).c_str();
}

void TaskFlowWrapper::setProperty(const char* key, const char* value)
{
    if (!_taskFlow) {
        return;
    }
    return _taskFlow->setProperty(key, value);
}

void TaskFlowWrapper::bindLua(lua_State* state)
{
    assert(state);
    ELuna::registerClass<TaskFlowWrapper>(state, "TaskFlowWrapper", ELuna::constructor<TaskFlowWrapper>);
    ELuna::registerMethod<TaskFlowWrapper>(state, "isValid", &TaskFlowWrapper::isValid);
    ELuna::registerMethod<TaskFlowWrapper>(state, "createTask", &TaskFlowWrapper::createTask);
    ELuna::registerMethod<TaskFlowWrapper>(state, "getTask", &TaskFlowWrapper::getTask);
    ELuna::registerMethod<TaskFlowWrapper>(state, "finishTask", &TaskFlowWrapper::finishTask);
    ELuna::registerMethod<TaskFlowWrapper>(state, "getTaskInFlow", &TaskFlowWrapper::getTaskInFlow);
    ELuna::registerMethod<TaskFlowWrapper>(state, "addUpstreamFlow", &TaskFlowWrapper::addUpstreamFlow);
    ELuna::registerMethod<TaskFlowWrapper>(state, "addUpstreamFlowById", &TaskFlowWrapper::addUpstreamFlowById);
    ELuna::registerMethod<TaskFlowWrapper>(state, "addFriendFlowId", &TaskFlowWrapper::addFriendFlowId);
    ELuna::registerMethod<TaskFlowWrapper>(state, "addFriendFlow", &TaskFlowWrapper::addFriendFlow);
    ELuna::registerMethod<TaskFlowWrapper>(state, "isFlowRunning", &TaskFlowWrapper::isFlowRunning);
    ELuna::registerMethod<TaskFlowWrapper>(state, "isFlowStopped", &TaskFlowWrapper::isFlowStopped);
    ELuna::registerMethod<TaskFlowWrapper>(state, "isFlowSuspended", &TaskFlowWrapper::isFlowSuspended);
    ELuna::registerMethod<TaskFlowWrapper>(state, "isFlowFinish", &TaskFlowWrapper::isFlowFinish);
    ELuna::registerMethod<TaskFlowWrapper>(state, "hasRuntimeError", &TaskFlowWrapper::hasRuntimeError);
    ELuna::registerMethod<TaskFlowWrapper>(state, "hasFatalError", &TaskFlowWrapper::hasFatalError);
    ELuna::registerMethod<TaskFlowWrapper>(state, "getFlowId", &TaskFlowWrapper::getFlowId);
    ELuna::registerMethod<TaskFlowWrapper>(state, "openApi", &TaskFlowWrapper::openApi);
    ELuna::registerMethod<TaskFlowWrapper>(state, "setGlobalTaskParam", &TaskFlowWrapper::setGlobalTaskParam);
    ELuna::registerMethod<TaskFlowWrapper>(state, "setError", &TaskFlowWrapper::setError);
    ELuna::registerMethod<TaskFlowWrapper>(state, "setPublic", &TaskFlowWrapper::setPublic);
    ELuna::registerMethod<TaskFlowWrapper>(state, "setProperty", &TaskFlowWrapper::setProperty);
    ELuna::registerMethod<TaskFlowWrapper>(state, "getProperty", &TaskFlowWrapper::getProperty);
    ELuna::registerMethod<TaskFlowWrapper>(state, "getFlowProperty", &TaskFlowWrapper::getFlowProperty);
    ELuna::registerMethod<TaskFlowWrapper>(state, "getFlowStatus", &TaskFlowWrapper::getFlowStatus);
    ELuna::registerMethod<TaskFlowWrapper>(state, "suspendFlow", &TaskFlowWrapper::suspendFlow);
    ELuna::registerMethod<TaskFlowWrapper>(state, "resumeFlow", &TaskFlowWrapper::resumeFlow);
    ELuna::registerMethod<TaskFlowWrapper>(state, "stopFlow", &TaskFlowWrapper::stopFlow);
    ELuna::registerMethod<TaskFlowWrapper>(state, "addTag", &TaskFlowWrapper::addTag);
}

#undef LOG_PREFIX

}} // namespace build_service::admin
